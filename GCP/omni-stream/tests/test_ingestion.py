from data_ingestion.stream_ingestion import publish_message  # Import the function

def test_publish_message():
    message_data = {"field1": "test", "field2": "data"}
    result = publish_message(message_data)  

    assert isinstance(result, str)  # Assuming it returns a message ID (string)
    assert result != ""  # Ensure it's not an empty response
