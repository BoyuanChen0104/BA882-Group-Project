from apify_client import ApifyClient

# Initialize the ApifyClient with API token
client = ApifyClient("apify_api_EkelRouO2zZ7vY0PzeGUJupeTFreRa1o974d")

run_input = {
    "language": "en",
    "maxReviews": 100,
    "personalData": true,
    "reviewsStartDate": "2024-10-01",
    "startUrls": [
        {
            "url": "https://www.google.com/maps/place/Jumbo+Seafood/@42.350931,-71.0627748,17z/data=!4m8!3m7!1s0x89e37a7847ac82ad:0x59ee82a6474ad485!8m2!3d42.350931!4d-71.0601999!9m1!1b1!16s%2Fg%2F1tjt2z0n?hl=en-GB&entry=ttu&g_ep=EgoyMDI0MDkxOC4xIKXMDSoASAFQAw%3D%3D"
        },
        {
            "url": "https://www.google.com/maps/place/Rowayton+Seafood/@41.0640248,-74.4434072,9z/data=!4m11!1m3!2m2!1sseafood+restaurant+near+New+England!6e5!3m6!1s0x89e81fc9005d651d:0x197740d3504cf794!8m2!3d41.0640248!4d-73.4443415!15sCiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBOZXcgRW5nbGFuZFolIiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBuZXcgZW5nbGFuZJIBEnNlYWZvb2RfcmVzdGF1cmFudOABAA!16s%2Fg%2F1thvtqxf?authuser=0&entry=ttu&g_ep=EgoyMDI0MDkyMy4wIKXMDSoASAFQAw%3D%3D"
        },
        {
            "url": "https://www.google.com/maps/place/Abe+%26+Louie's/@42.349138,-72.2021125,9z/data=!4m11!1m3!2m2!1sseafood+restaurant+near+New+England!6e5!3m6!1s0x89e37a0ef7c51c4d:0x3b643d1ee9cd8345!8m2!3d42.349138!4d-71.081507!15sCiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBOZXcgRW5nbGFuZFolIiNzZWFmb29kIHJlc3RhdXJhbnQgbmVhciBuZXcgZW5nbGFuZJIBC3N0ZWFrX2hvdXNl4AEA!16s%2Fg%2F1tl1pg4w?authuser=0&entry=ttu&g_ep=EgoyMDI0MDkyMy4wIKXMDSoASAFQAw%3D%3D"
        }
    ]
}

run = client.actor("Xb8osYTtOjlsgI6k9").call(run_input=run_input)

for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    print(item)



