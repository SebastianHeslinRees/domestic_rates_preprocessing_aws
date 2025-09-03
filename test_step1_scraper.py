from step1_ons_scraper import lambda_handler

mock_event = {}
mock_context = {}

if __name__ == "__main__":
    result = lambda_handler(mock_event, mock_context)
    for link in result['download_links']:
        print(link)

