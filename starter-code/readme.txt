metadata.json stores all the files and their complete offsets in
metadata['files'].

The files of a node and their offsets are stored under the 'files' key for each
node in metadata['nodes'].

client.py contains get_current_file_data(), which gets the current files of the
client and their offsets.

client.py also contains get_missing_file_data(), which gets the missing or
incomplete files of the client and their missing offsets.
