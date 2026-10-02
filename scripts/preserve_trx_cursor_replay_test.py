"""Local external-replay substitute for cursor integration tests, never product code.

Capture the native live ID set before disconnect, replay the recorded definitions
on the restored backend, then call the explicit kernel cursor API. No SELECT is
executed to recreate a preserved result.
"""
from preserve_trx_classic_client import ParameterClient


class ReplayClient(ParameterClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prepared_queries = {}
        self.replay_queries = None

    def prepare(self, query):
        statement = super().prepare(query)
        self.prepared_queries[statement] = query
        return statement

    def freeze_replay(self, observer):
        live = observer.query(
            'SELECT p.STATEMENT_ID FROM performance_schema.prepared_statements_instances p '
            'JOIN performance_schema.threads t ON p.OWNER_THREAD_ID=t.THREAD_ID '
            f'WHERE t.PROCESSLIST_ID={self.id} ORDER BY p.STATEMENT_ID')
        self.replay_queries = {int(row[0]): self.prepared_queries[int(row[0])] for row in live}


def replay_statements(source, target, prepare_statement=None):
    assert source.replay_queries is not None, 'missing source PS lifetime snapshot'
    prepare_statement = prepare_statement or target.prepare
    target.query("SET SESSION debug='+d,preserve_cursor_test_command'")
    next_id = 1
    for statement, query in sorted(source.replay_queries.items()):
        while next_id < statement:
            filler = target.prepare('SELECT 0')
            assert filler == next_id
            target.close_statement(filler)
            next_id += 1
        assert prepare_statement(query) == statement, 'external replay changed original PS ID'
        target.query(f'DO /* preserve_cursor_attach:{statement} */ 0')
        next_id = statement + 1
    target.query("SET SESSION debug='-d,preserve_cursor_test_command'")
