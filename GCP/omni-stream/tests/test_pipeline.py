from data_processing.beam_pipeline import run_beam_pipeline  # Import the function

def test_pipeline():
    result = run_beam_pipeline()
    
    assert isinstance(result, list)  # Ensure it returns a list (or expected data type)
    assert len(result) > 0  # Ensure pipeline processes some records
    assert all("expected_field" in record for record in result)  # Check required fields exist
