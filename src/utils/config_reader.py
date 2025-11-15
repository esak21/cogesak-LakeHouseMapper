import yaml


def read_config(file_path):
    try:
        with open(file_path, "r") as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        print("Error : Check the File Path")
    except yaml.YAMLError as e:
        print(f"Error : File {file} parsing Error {e}")

    return config


if __name__ == "__main__":
    env = "dev"
    config = read_config(
        f"/Users/esak/Documents/Esakki/code_base/current_project/3LayersArch/configs/{env}.yaml"
    )
    print(config["db"])
