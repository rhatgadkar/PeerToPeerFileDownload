from json import load
from os import listdir, curdir
from argparse import ArgumentParser

def get_current_file_data(node_number, metadata):
    """
    Get a dict of a node's files and their start and end offsets.

    e.g., {'n0.f1.txt': [(0, 200), (500, 800)]}
    """
    to_return = {}
    for file_dict in metadata['nodes'][node_number]['files']:
        offsets_list = []
        for offset_dict in file_dict['data']:
            offset_tup = (offset_dict['start_off'], offset_dict['end_off'])
            offsets_list.append(offset_tup)
        to_return[ file_dict['name'] ] = offsets_list
    return to_return

def get_missing_data_tuples(all_file_offsets, curr_file_offsets):
    """
    Get a list of missing tuples of start and end offsets.

    e.g., all_file_offsets = [(0, 800)] and
          curr_file_offsets = [(0, 200), (500, 800)]
    The function will return: [(201, 499)]
    """

    def get_tuple_containing_curr(all_offsets, curr_offset):
        """
        Get a tuple from all_offsets that contains curr_offset.

        e.g., all_offsets = [(500, 600), (700, 1000)] and
              curr_offset = (500, 550)
        The function will return: (500, 600)
        """
        for offset in all_offsets:
            if curr_offset[0] >= offset[0] and offset[1] >= curr_offset[1]:
                return offset
        return None

    missing_file_offsets = all_file_offsets
    for curr_file_offset in curr_file_offsets:
        all_file_offset = get_tuple_containing_curr(missing_file_offsets,
                curr_file_offset)
        if not all_file_offset:
            return []
        firstHalf = (all_file_offset[0], curr_file_offset[0] - 1)
        secHalf = (curr_file_offset[1] + 1, all_file_offset[1])
        missing_file_offsets.remove(all_file_offset)
        if firstHalf[0] != firstHalf[1] + 1:
            missing_file_offsets.append(firstHalf)
        if secHalf[0] - 1 != secHalf[1]:
            missing_file_offsets.append(secHalf)
    return missing_file_offsets

def get_missing_file_data(node_number, metadata):
    """
    Get a dict of a node's missing files and their missing start and end
    offsets.

    e.g., {'n0.f1.txt': [(201, 499)]}
    """
    # all_files_dict stores a dict of all complete files with their start and
    # end offsets.
    all_files_dict = {}
    for file_dict in metadata['files']:
        offsets_list = []
        for offset_dict in file_dict['data']:
            offset_tup = (offset_dict['start_off'], offset_dict['end_off'])
            offsets_list.append(offset_tup)
        all_files_dict[ file_dict['name'] ] = offsets_list

    # current_files_dict stores the node's current files with their start and
    # end offsets.
    current_files_dict = get_current_file_data(node_number, metadata)

    to_return = {}
    # Iterate through all_files_dict and check these cases:
    # 1. File does not exist in current_files_dict -> append the file and its
    #    offsets to to_return.
    # 2. File exists in current_files_dict but the offsets don't match -> append
    #    the file and the offsets that don't match to to_return.
    # 3. File exists in current_files_dict and the offsets match -> continue
    for file_name, file_offsets in all_files_dict.iteritems():
        if file_name not in current_files_dict:
            to_return[file_name] = file_offsets
        elif file_offsets != current_files_dict[file_name]:
            missing_data_tuples = get_missing_data_tuples(file_offsets,
                    current_files_dict[file_name])
            to_return[file_name] = missing_data_tuples
    return to_return

def main(node_number):
    with open('metadata.json', 'r') as f:
        metadata = load(f)
    current_files = get_current_file_data(node_number, metadata)
    missing_files = get_missing_file_data(node_number, metadata)
    print missing_files

parser = ArgumentParser(description='node number')
parser.add_argument('node_number', nargs=1, type=int, help='node number')
args = parser.parse_args()
main(args.node_number[0])
