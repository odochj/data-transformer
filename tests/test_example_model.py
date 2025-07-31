from sqlmesh.core.context import Context

def test_example_model():
    context = Context()
    df = context.evaluate('example_model')
    assert df.shape[0] > 0
    assert "id" in df.columns
