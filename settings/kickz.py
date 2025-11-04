countries = [
    "AT", 
    "FR", 
    "DE",
    "CH",
    "GB",
    # "IT", 
    # "NL", 
    # "NO", 
    # "ES", 
    # "BE", 
    # "DK", 
    # "FI", 
    # "SE", 
]


# Google Sheets
gs_path_token = './auth_files/token.pickle'
gs_path_client_secret = './auth_files/client_secret_708086849726-edgo6g4pigkf5rj0qc52rir18oso0kto.apps.googleusercontent.com.json'
gs_spreadsheet_id = '1PHzUwQEZ5gLmf32D3Akx_qL5iFta8HuCnZWtWVb3bTk'

# Sentry
sentry_dsn = 'https://d4861196a0bac78660f394c1bc225891@o504927.ingest.us.sentry.io/4509962027597824'

# Google Service account
google_service_account_json_path = "auth_files/eas-core-34afc8b13f75.json"

# Export to productions
export_container_name = 'hybris'
export_blob_name = "master/hotfolder/kickz/prices-{ts_millis}.csv"

# # D2 - TEST ENV
export_connection_string = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=o3w4n3fotw789gttys79amu;"
    "AccountKey=ltkXAjB76EPhkIfKM9Drmnn9k8XP8Zvb4d5ymrmQCGNbmFWeHHYGZvtB7VJuwnRiZHc64vN62cdO+AStQeCLVg==;"
    "EndpointSuffix=core.windows.net"
)

# P1 - PROD ENV
# export_connection_string = (
#     "DefaultEndpointsProtocol=https;"
#     "AccountName=xumamovq8lqzp9sgl7i2chl;"
#     "AccountKey=pcs8r5A8P7f11Nkz2CctsLLjWWkOZl562WW/6iMXaCmVAeZd+sjMiXxP6fzN1YkEybW5ESeqh8eo+AStje2qLA==;"
#     "EndpointSuffix=core.windows.net"
# )

# Export to 11TS Blob storage for dashboard
dashboard_export_connection_string = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=dlssynone11001;"
    "AccountKey=2VrwVvGrqX2XI/qjmaNuDyXQ3Pmnh1g67gq0rEAF3PQXFzoO6Rcl58de6zRGENMFFGjdcbgcxItP+AStCqWvgQ==;"
    "EndpointSuffix=core.windows.net"
)