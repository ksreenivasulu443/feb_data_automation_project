import requests
import yaml


def read_yaml_from_github(owner, repo, filepath):
    # GitHub raw file URL
    raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/master/{filepath}"

    try:
        # Fetch YAML file content from GitHub
        response = requests.get(raw_url)
        response.raise_for_status()  # Raise an exception for bad responses

        # Parse YAML content
        yaml_content = yaml.safe_load(response.text)

        return yaml_content

    except requests.exceptions.RequestException as e:
        print("Error fetching YAML file:", e)
        return None


# Example usage
owner = "ksreenivasulu443"
repo = "feb_data_automation_project"
filepath = r"C:\Users\A4952\PycharmProjects\feb_data_automation_project\.github\workflows\python-app.yml"

yaml_data = read_yaml_from_github(owner, repo, filepath)
if yaml_data:
    print("YAML file content:")
    print(yaml_data)
else:
    print("Error occurred while fetching YAML file.")


