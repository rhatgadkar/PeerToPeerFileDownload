from kazoo.client import KazooClient
from kazoo.protocol.states import ZnodeStat
import os
import argparse
import threading

NODE_FILES_DIR = '/node-files'
ALL_FILES_DIR = '/all-files'
ALIVE_NODES_DIR = '/alive-nodes'

def parse_offsets(offset_input):
    """
    Example:
    offset_input: 0,200;800,1000

    Return: [(0, 200), (800, 1000)]
    """
    to_return = []
    for offset_range in offset_input.split(';'):
        start, end = offset_range.split(',')
        to_return.append( (int(start), int(end)) )
    return to_return

def get_current_file_data(node_ip, zk_handle):
    """
    Get a dict of a node's files and their start and end offsets.

    e.g., {'n0.f1.txt': [(0, 200), (500, 800)]}
    """
    to_return = {}
    node_files_path = os.path.join(NODE_FILES_DIR, node_ip)
    node_files = zk_handle.get_children(node_files_path)
    for file_name in node_files:
        file_path = os.path.join(node_files_path, file_name)
        file_data, file_stat = zk_handle.get(file_path)
        to_return[file_name] = parse_offsets(file_data)
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

def get_missing_file_data(node_ip, zk_handle):
    """
    Get a dict of a node's missing files and their missing start and end
    offsets.

    e.g., {'n0.f1.txt': [(201, 499)]}
    """
    # all_files_dict stores a dict of all complete files with their start and
    # end offsets.
    all_files_dict = {}
    all_files = zk_handle.get_children(ALL_FILES_DIR)
    for file_name in all_files:
        file_path = os.path.join(ALL_FILES_DIR, file_name)
        file_data, file_stat = zk_handle.get(file_path)
        all_files_dict[file_name] = parse_offsets(file_data)

    # current_files_dict stores the node's current files with their start and
    # end offsets.
    current_files_dict = get_current_file_data(node_ip, zk_handle)

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

def get_nodes_with_missing_files(node_ip, zk_handle, missing_files):
    """
    Get a dict of nodes that contain the missing files and missing bytes of
    files for a given node.

    e.g., missing_files = {'n1.f1.txt': [(9, 9)], 'n0.f1.txt': [(201, 400)]}
          for node 0, and node 1 contains these files with the provided byte
          ranges:
          {'n1.f1.txt': [(0, 9)], 'n0.f1.txt': [(0, 400)]}
    The function will return:
    { 1: {'n1.f1.txt': [(9, 9)], 'n0.f1.txt': [(201, 400)]} }
    """

    def get_byte_range_from_node(all_offsets, curr_offset):
        """
        Get a tuple within all_offsets that contains a byte range within
        curr_offset.

        e.g., all_offsets = [(500, 600), (700, 1000)] and
              curr_offset = (500, 550)
        The function will return: (500, 550)

        e.g., all_offsets = [(500, 699), (700, 1000)] and
              curr_offset = (600, 800)
        The function will return: (600, 699)
        """
        for offset in all_offsets:
            start_offset = None
            end_offset = None
            if curr_offset[0] >= offset[0] and curr_offset[0] <= offset[1]:
                start_offset = curr_offset[0]
            if offset[1] <= curr_offset[1] and start_offset:
                end_offset = offset[1]
            elif offset[1] >= curr_offset[1] and start_offset:
                end_offset = curr_offset[1]
            if start_offset and end_offset:
                return (start_offset, end_offset)
        return None

    to_return = {}
    for missing_file, missing_byte_ranges in missing_files.iteritems():
        node_files = zk_handle.get_children(NODE_FILES_DIR)
        for node_file in node_files:
            if node_file == node_ip:
                continue
            node_ip_files = get_current_file_data(node_file, zk_handle)
            if missing_file not in node_ip_files:
                # node node_ip does not contain missing_file
                continue
            # check if node node_ip contains the missing_file and missing_bytes
            for missing_byte_range in missing_byte_ranges:
                byte_range = get_byte_range_from_node(
                        node_ip_files[missing_file], missing_byte_range)
                if not byte_range:
                    # node node_ip does not contain the missing_file with the
                    # byte range
                    continue
                if node_file not in to_return:
                    to_return[node_file] = {}
                if missing_file not in to_return[node_file]:
                    to_return[node_file][missing_file] = []
                to_return[node_file][missing_file].append(byte_range)
    return to_return

def update_file_offsets(curr_file_offsets, rcvd_offset):
    """
    If curr_file_offsets = [(0, 200), (400, 499)] and rcvd_offset = (500, 599),
    then this function should return:
    [(0, 200), (400, 599)]

    If curr_file_offsets = [(100, 200), (400, 800)] and rcvd_offset = (0, 99),
    then this function should return:
    [(0, 200), (400, 800)]

    If curr_file_offsets = [(100, 200), (400, 800)] and
    rcvd_offset = (201, 300), then this function should return:
    [(100, 300), (400, 800)]

    If curr_file_offsets = [(100, 199), (400, 800)] and
    rcvd_offset = (200, 399), then this function should return:
    [(100, 800)]

    If curr_file_offsets = [(0, 0)] and rcvd_offset = (1, 1),
    then this function should return:
    [(0, 1)]
    """
    new_file_offsets = []
    rcvd_start = rcvd_offset[0]
    rcvd_end = rcvd_offset[1]
    prev_added_offset = None
    for curr_file_offset in curr_file_offsets:
        curr_file_start = curr_file_offset[0]
        curr_file_end = curr_file_offset[1]
        if prev_added_offset and prev_added_offset not in curr_file_offsets:
            prev_start = prev_added_offset[0]
            prev_end = prev_added_offset[1]
            # Example:
            # curr_file_offsets = [(100, 199), (400, 800)]
            #
            # 1st it: new_file_offsets = [] and prev_added_offset = None and
            #         rcvd_offset = (200, 399) and curr_file_offset = (100, 199)
            #         Result: new_file_offsets = [(100, 399)] and
            #                 prev_added_offset = (100, 399)
            #
            # (this if-statement will execute in the 2nd it)
            # 2nd it: new_file_offsets = [(100, 399)] and
            #         prev_added_offset = (100, 399) and
            #         rcvd_offset = (200, 399) and
            #         curr_file_offset = (400, 800)
            #         Result: new_file_offsets = [(100, 800)] and
            #                 prev_added_offset = (100, 800)
            if prev_end + 1 == curr_file_start:
                new_file_offsets.pop()  # delete prev_added_offset
            rcvd_start = prev_start
            rcvd_end = prev_end
                #new_file_offset = (prev_start, curr_file_end)
        if rcvd_end + 1 == curr_file_start:
            # Example: curr_file_offsets = [(100, 200)] and
            #          rcvd_offset = (0, 99)
            # Result: new_file_offsets = [(0, 200)]
            new_file_offset = (rcvd_start, curr_file_end)
        elif curr_file_end + 1 == rcvd_start:
            # Example: curr_file_offsets = [(100, 200)] and
            #          rcvd_offset = (201, 300)
            # Result: new_file_offsets = [(100, 300)]
            new_file_offset = (curr_file_start, rcvd_end)
        else:
            new_file_offset = curr_file_offset
        new_file_offsets.append(new_file_offset)
        prev_added_offset = new_file_offset
    list.sort(new_file_offsets)
    return new_file_offsets

def thread_func(args):
    node_ip = args[0]
    zk_handle = args[1]
    thread1_data = args[2]
    thread1_data_lock = args[3]
    thread2_data = args[4]
    thread2_data_lock = args[5]
    thread_name = threading.current_thread().getName()
    while True:
        current_files = get_current_file_data(node_ip, zk_handle)
        print '%s -> Node %s\'s current files: %s' % (thread_name, node_ip,
                str(current_files))
        missing_files = get_missing_file_data(node_ip, zk_handle)
        print '%s -> Node %s\'s missing files: %s' % (thread_name, node_ip,
                str(missing_files))
        nodes_with_missing_files = get_nodes_with_missing_files(node_ip,
                zk_handle, missing_files)
        print '%s -> Nodes that contain the missing files of Node %s: %s' % (
                thread_name, node_ip, str(nodes_with_missing_files))
        # TODO: order the nodes with the missing files based on number of
        #       missing files

def main(node_ip):
    zk_handle = KazooClient(hosts='127.0.0.1:2181')
    try:
        zk_handle.start()
    except:
        raise Exception('Could not establish connection to ZK server')

    thread1_data = {}
    thread1_data_lock = threading.Lock()
    thread2_data = {}
    thread2_data_lock = threading.Lock()

    # the threads should run forever until a SIGINT
    thread1 = threading.Thread(target=thread_func, name='thread1', args=(
            node_ip, zk_handle, thread1_data, thread1_data_lock, thread2_data,
            thread2_data_lock))
    thread2 = threading.Thread(target=thread_func, name='thread2', args=(
            node_ip, zk_handle, thread1_data, thread1_data_lock, thread2_data,
            thread2_data_lock))
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

parser = argparse.ArgumentParser(description='node IP')
parser.add_argument('node_ip', nargs=1, type=str, help='node IP')
args = parser.parse_args()
main(args.node_ip[0])
