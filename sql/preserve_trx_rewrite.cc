/* Copyright (c) 2026, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "my_config.h"

#include "sql/sql_rewrite.h"

#include <string>

#include "m_ctype.h"
#include "m_string.h"
#include "mysql_version.h"
#include "sql/preserve_trx.h"
#include "sql/sql_const.h"
#include "sql_string.h"

namespace {

bool raw_sql_is_ident_char(char c) {
  return my_isalnum(&my_charset_latin1, c) || c == '_' || c == '$';
}

bool raw_sql_starts_dash_comment(const char *query, size_t query_length,
                                 size_t position) {
  return position + 2 < query_length && query[position] == '-' &&
         query[position + 1] == '-' &&
         (my_isspace(&my_charset_latin1, query[position + 2]) ||
          my_iscntrl(&my_charset_latin1, query[position + 2]));
}

bool raw_sql_read_version_comment(const char *query, size_t query_length,
                                  size_t position, ulong *version) {
  if (position + 8 > query_length || query[position] != '/' ||
      query[position + 1] != '*' || query[position + 2] != '!')
    return false;

  ulong value = 0;
  for (size_t i = 0; i < 5; ++i) {
    const char c = query[position + 3 + i];
    if (!my_isdigit(&my_charset_latin1, c)) return false;
    value = value * 10 + static_cast<ulong>(c - '0');
  }
  *version = value;
  return true;
}

void raw_sql_skip_spaces_and_comments(const char *query, size_t query_length,
                                      size_t *position) {
  while (*position < query_length) {
    if (my_isspace(&my_charset_latin1, query[*position])) {
      ++*position;
      continue;
    }

    if (query[*position] == '#') {
      while (*position < query_length && query[*position] != '\n' &&
             query[*position] != '\r') {
        ++*position;
      }
      continue;
    }

    if (raw_sql_starts_dash_comment(query, query_length, *position)) {
      *position += 2;
      while (*position < query_length && query[*position] != '\n' &&
             query[*position] != '\r') {
        ++*position;
      }
      continue;
    }

    if (*position + 1 < query_length && query[*position] == '*' &&
        query[*position + 1] == '/') {
      *position += 2;
      continue;
    }

    if (*position + 1 < query_length && query[*position] == '/' &&
        query[*position + 1] == '*') {
      ulong version = 0;
      if (raw_sql_read_version_comment(query, query_length, *position,
                                       &version) &&
          version <= MYSQL_VERSION_ID) {
        *position += 8;  // /*! + five version digits.
        continue;
      }

      *position += 2;
      while (*position + 1 < query_length &&
             !(query[*position] == '*' && query[*position + 1] == '/')) {
        ++*position;
      }
      if (*position + 1 < query_length) {
        *position += 2;
        continue;
      }
      *position = query_length;
      return;
    }

    return;
  }
}

bool raw_sql_match_keyword(const char *query, size_t query_length,
                           size_t *position, const char *keyword,
                           size_t keyword_length) {
  raw_sql_skip_spaces_and_comments(query, query_length, position);
  if (query_length - *position < keyword_length) return false;
  for (size_t i = 0; i < keyword_length; ++i) {
    if (my_toupper(&my_charset_latin1, query[*position + i]) != keyword[i])
      return false;
  }
  const size_t end = *position + keyword_length;
  if (end < query_length && raw_sql_is_ident_char(query[end])) return false;
  *position = end;
  return true;
}

int raw_sql_hex_digit_value(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

bool raw_sql_decode_hex(const char *literal, size_t length,
                        std::string *decoded) {
  if (literal == nullptr || decoded == nullptr) return false;
  decoded->clear();
  if ((length % 2) != 0) {
    return false;
  }
  decoded->reserve(length / 2);

  for (size_t position = 0; position < length; position += 2) {
    const int high = raw_sql_hex_digit_value(literal[position]);
    const int low = raw_sql_hex_digit_value(literal[position + 1]);
    if (high < 0 || low < 0) return false;
    decoded->push_back(static_cast<char>((high << 4) | low));
  }
  return true;
}

bool raw_sql_decode_binary(const char *literal, size_t length,
                           std::string *decoded) {
  if (literal == nullptr || decoded == nullptr) return false;
  decoded->clear();
  decoded->reserve((length + 7) / 8);

  unsigned char value = 0;
  size_t bit_count = 0;
  for (size_t i = 0; i < length; ++i) {
    if (literal[i] != '0' && literal[i] != '1') return false;
    value = static_cast<unsigned char>((value << 1) | (literal[i] - '0'));
    ++bit_count;
    if (bit_count == 8) {
      decoded->push_back(static_cast<char>(value));
      value = 0;
      bit_count = 0;
    }
  }
  if (bit_count != 0) {
    value = static_cast<unsigned char>(value << (8 - bit_count));
    decoded->push_back(static_cast<char>(value));
  }
  return true;
}

bool raw_sql_skip_quoted(const char *query, size_t query_length,
                         size_t *position, char quote,
                         bool backslash_escapes) {
  if (*position >= query_length || query[*position] != quote) return false;
  ++*position;
  while (*position < query_length) {
    const char c = query[*position];
    ++*position;
    if (backslash_escapes && c == '\\') {
      if (*position < query_length) ++*position;
      continue;
    }
    if (c == quote) {
      if (*position < query_length && query[*position] == quote) {
        ++*position;
        continue;
      }
      return true;
    }
  }
  return false;
}

std::string raw_sql_unquote_string(const char *query, size_t start, size_t end,
                                   bool backslash_escapes) {
  std::string token;
  if (query == nullptr || start >= end) return token;
  const char quote = query[start];
  if (quote != '\'' && quote != '"') return token;
  for (size_t i = start + 1; i + 1 < end; ++i) {
    const char c = query[i];
    if (backslash_escapes && c == '\\' && i + 1 < end - 1) {
      token.push_back(query[++i]);
      continue;
    }
    if (c == quote && i + 1 < end - 1 && query[i + 1] == quote) {
      token.push_back(quote);
      ++i;
      continue;
    }
    token.push_back(c);
  }
  return token;
}

bool raw_sql_parse_resume_token(const char *query, size_t query_length,
                                size_t *literal_start, size_t *literal_end,
                                std::string *token) {
  if (query == nullptr || literal_start == nullptr || literal_end == nullptr ||
      token == nullptr)
    return false;

  size_t pos = 0;
  raw_sql_skip_spaces_and_comments(query, query_length, &pos);
  if (!raw_sql_match_keyword(query, query_length, &pos,
                             STRING_WITH_LEN("RESUME")) ||
      !raw_sql_match_keyword(query, query_length, &pos,
                             STRING_WITH_LEN("PRESERVED")) ||
      !raw_sql_match_keyword(query, query_length, &pos,
                             STRING_WITH_LEN("TRANSACTION"))) {
    return false;
  }

  raw_sql_skip_spaces_and_comments(query, query_length, &pos);
  if (pos >= query_length) return false;

  *literal_start = pos;
  token->clear();
  if ((query[pos] == 'x' || query[pos] == 'X' || query[pos] == 'b' ||
       query[pos] == 'B') &&
      pos + 1 < query_length && query[pos + 1] == '\'') {
    const char prefix = query[pos];
    size_t scan = pos + 1;
    if (!raw_sql_skip_quoted(query, query_length, &scan, '\'', true))
      return false;
    *literal_end = scan;
    const char *content = query + pos + 2;
    const size_t content_length = scan - pos - 3;
    return prefix == 'x' || prefix == 'X'
               ? raw_sql_decode_hex(content, content_length, token)
               : raw_sql_decode_binary(content, content_length, token);
  }

  if (query[pos] == '\'' || query[pos] == '"') {
    const char quote = query[pos];
    size_t scan = pos;
    if (!raw_sql_skip_quoted(query, query_length, &scan, quote, true))
      return false;
    *literal_end = scan;
    *token = raw_sql_unquote_string(query, pos, scan, true);
    return true;
  }

  if (query[pos] == '0' && pos + 2 < query_length &&
      (query[pos + 1] == 'x' || query[pos + 1] == 'X')) {
    size_t scan = pos + 2;
    while (scan < query_length &&
           my_isxdigit(&my_charset_latin1, query[scan])) {
      ++scan;
    }
    if (scan == pos + 2) return false;
    *literal_end = scan;
    return raw_sql_decode_hex(query + pos + 2, scan - pos - 2, token);
  }

  if (query[pos] == '0' && pos + 2 < query_length &&
      (query[pos + 1] == 'b' || query[pos + 1] == 'B')) {
    size_t scan = pos + 2;
    while (scan < query_length && (query[scan] == '0' || query[scan] == '1')) {
      ++scan;
    }
    if (scan == pos + 2) return false;
    *literal_end = scan;
    return raw_sql_decode_binary(query + pos + 2, scan - pos - 2, token);
  }

  return false;
}

}  // namespace

bool mysql_rewrite_resume_preserved_transaction_raw(
    THD *, const char *query, size_t query_length, String *rewritten_query) {
  if (query == nullptr || rewritten_query == nullptr) return false;

  size_t literal_start = 0;
  size_t literal_end = 0;
  std::string token;
  if (!raw_sql_parse_resume_token(query, query_length, &literal_start,
                                  &literal_end, &token)) {
    return false;
  }

  rewritten_query->length(0);
  rewritten_query->append(query, literal_start);
  const std::string redacted = preserved_trx_redacted_token(token);
  rewritten_query->append(STRING_WITH_LEN("'"));
  rewritten_query->append(redacted.c_str(), redacted.length());
  rewritten_query->append(STRING_WITH_LEN("'"));
  rewritten_query->append(query + literal_end, query_length - literal_end);
  return true;
}
