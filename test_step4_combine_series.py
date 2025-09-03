from step4_combine_series import handler

if __name__ == "__main__":
    # Simulated AWS Lambda event
    mock_event = {}

    # Simulated context (not used here, but Lambda always passes it)
    mock_context = None

    # Call the handler
    result = handler(mock_event, mock_context)

    print("\n--- Lambda Result ---")
    print(result)