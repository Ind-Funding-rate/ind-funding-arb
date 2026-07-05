import requests

def get_current_ip():
    """Get current public IPv4 address."""
    response = requests.get("https://api.ipify.org?format=json", timeout=10)
    return response.json()["ip"]

if __name__ == "__main__":
    ip = get_current_ip()
    print(f"\nYour current public IPv4 address is:\n")
    print(f"   {ip}\n")
    print("Copy this into: Pi42 -> Profile -> API Keys -> Edit -> Allowed IPs\n")
