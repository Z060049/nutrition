import os
import shutil

def cleanup_folders():
    """Clear all data in nutrition_data, processed_data, and options folders"""
    folders_to_clean = ['nutrition_data', 'processed_data', 'options']
    
    for folder in folders_to_clean:
        if os.path.exists(folder):
            print(f"Cleaning {folder} folder...")
            shutil.rmtree(folder)
            print(f"Cleaned {folder} folder")
        else:
            print(f"{folder} folder does not exist")

if __name__ == "__main__":
    cleanup_folders() 