import os
import shutil
import argparse

def filter_and_copy_files(directory_path, target_file, destination_directory):
    # Read the list of strings from the target file
    with open(target_file, 'r') as file:
        target_strings = set(line.strip() for line in file if line.strip())

    # Ensure the destination directory exists
    os.makedirs(destination_directory, exist_ok=True)

    # Iterate over the files in the directory
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        
        # Skip directories, only process files
        if os.path.isfile(file_path):
            try:
                with open(file_path, 'r') as current_file:
                    file_content = current_file.read()

                # Check if none of the target strings are in the file content
                if not any(target in file_content for target in target_strings):
                    # Copy the file if it does not match any string in the target file
                    destination_path = os.path.join(destination_directory, filename)
                    shutil.copy(file_path, destination_path)
                    print(f"Copied: {filename} to {destination_directory}")
                else:
                    print(f"Skipped: {filename}")
            except Exception as e:
                print(f"Error reading {filename}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter and copy files in a directory based on a target file.")
    parser.add_argument("directory", help="Path to the directory containing files.")
    parser.add_argument("target_file", help="Path to the file containing newline-separated strings.")
    parser.add_argument("destination_directory", help="Path to the destination directory for matched files.")

    args = parser.parse_args()

    filter_and_copy_files(args.directory, args.target_file, args.destination_directory)
