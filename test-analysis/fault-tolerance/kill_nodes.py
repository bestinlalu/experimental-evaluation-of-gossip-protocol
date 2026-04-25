import requests

def kill_all_nodes():
    # Update the port if your Manager runs on a different one
    url = "http://localhost:9090/action/kill/all"

    try:
        print(f"🛑 Sending Kill signal to: {url}...")
        
        # Sending a POST request with an empty body
        response = requests.post(url)

        if response.status_code == 200 or response.status_code == 204:
            print("Success: All gossip node processes have been terminated.")
        else:
            print(f"Unexpected Status Code: {response.status_code}")
            print("Response:", response.text)

    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the Manager Service. Is it running?")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    kill_all_nodes()
