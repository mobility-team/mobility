import pickle
import hashlib

def is_update_needed(inputs, output_path):
    
    inputs_hash = pickle.dumps(inputs)
    inputs_hash = hashlib.sha256(inputs_hash).hexdigest()
    
    metadata_file_path = output_path.with_suffix(".metadata")
    
    if metadata_file_path.exists() is True:
        with open(metadata_file_path, "r") as f:
            existing_metadata_hash = f.read()
    else:
        existing_metadata_hash = None
    
    # Update the hash if the output is not up to date
    if existing_metadata_hash != inputs_hash:
        with open(metadata_file_path, "w") as f:
            f.write(inputs_hash)
            
    return existing_metadata_hash != inputs_hash, inputs_hash