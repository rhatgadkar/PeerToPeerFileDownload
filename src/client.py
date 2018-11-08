from kazoo.client import KazooClient
from kazoo.protocol.states import ZnodeStat
import kazoo.exceptions
import os
import argparse
import threading
from collections import OrderedDict
import random
import time

NODE_FILES_DIR = '/node-files'
ALL_FILES_DIR = '/all-files'
ALIVE_NODES_DIR = '/alive-nodes'

THREAD1 = 'thread1'
THREAD2 = 'thread2'

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

def get_znode_offsets(offset_input):
    """
    Example:
    offset_input: [(0, 200), (800, 1000)]
    
    Return: 0,200;800,1000
    """
    to_return = ''
    list.sort(offset_input)
    for i in range(len(offset_input)):
        curr_range = offset_input[i]
        if i > 0:
            to_return += ';'
        to_return += (str(curr_range[0]) + ',' + str(curr_range[1]))
    return to_return

def get_current_file_data(node_ip, zk_handle):
    """
    Get a dict of a node's files and their start and end offsets.

    e.g., { 'n0.f1.txt': (v#, [(0, 200), (500, 800)]) }
    """
    to_return = {}
    node_files_path = os.path.join(NODE_FILES_DIR, node_ip)
    node_files = zk_handle.get_children(node_files_path)
    for file_name in node_files:
        file_path = os.path.join(node_files_path, file_name)
        file_data, file_stat = zk_handle.get(file_path)
        to_return[file_name] = (file_stat.version, parse_offsets(file_data))
    return to_return

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

def get_missing_data_tuples(all_file_offsets, curr_file_offsets):
    """
    Get a list of missing tuples of start and end offsets.

    e.g., all_file_offsets = [(0, 800)] and
          curr_file_offsets = [(0, 200), (500, 800)]
    The function will return: [(201, 499)]
    """
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
        elif file_offsets != current_files_dict[file_name][1]:
            missing_data_tuples = get_missing_data_tuples(file_offsets,
                    current_files_dict[file_name][1])
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
                        node_ip_files[missing_file][1], missing_byte_range)
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
    if new_file_offsets == curr_file_offsets:
        # FIXME: this is incorrect; it should result in ordered incremental
        # offset ranges
        # Thread thread1 -> 172.16.1.10 successfully updated file n0.f1.txt with offsets: [(0, 200), (223, 322), (500, 800)]
        # Thread thread2 -> 172.16.1.10 trying to get file n0.f1.txt and offsets: (231, 330)
        # Thread thread2 -> 172.16.1.10 successfully updated file n0.f1.txt with offsets: [(0, 200), (223, 322), (231, 330), (500, 800)]

        # rcvd_offset does not affect the current offsets; so just append
        # rcvd_offset
        # Example: curr_file_offsets = [(0, 200), (500, 800)] and
        #          rcvd_offset = (298, 397)
        new_file_offsets.append(rcvd_offset)
        list.sort(new_file_offsets)
    return new_file_offsets


def get_random_file_offset(missing_files, other_thread_data):
    """
    Example missing_files:
    {'n1.f1.txt': [(0, 0), (80, 200)], 'n2.f2.txt': [(8, 9)]}

    Example other_thread_data:
    {'n1.f1.txt': [(0, 0), (93, 192)], 'n2.f2.txt': [(9, 9)]}

    Example return values:
    ('n1.f1.txt', (80, 92))
    ('n1.f1.txt', (193, 200))
    ('n2.f2.txt', (8, 8))

    Need to pick a random offset range of length at most 100 of a file which
    does not overlap with an offset from other_thread_data's offsets of the same
    file.
    """
    def add_missing_offset(missing_offsets, add_offset):
        """
        Example missing_offsets:
        [(0, 0), (80, 200)]

        Example add_offset:
        (93, 192)

        Changes missing_offsets to:
        [(0, 0), (80, 92), (193, 200)]

        add_offset will not be equal to any offset in missing_offsets, because
        that offset will be removed from missing_offsets.
        """
        # Example overlaps:
        # add_offset = (5, 10)  missing_offset = (0, 11)  -> [(0, 4), (11, 11)]
        # add_offset = (5, 11)  missing_offset = (0, 11)  -> [(0, 4)]
        # add_offset = (0, 5)   missing_offset = (0, 11)  -> [(6, 11)]
        missing_offset = get_tuple_containing_curr(missing_offsets, add_offset)
        add_start_off = add_offset[0]
        add_end_off = add_offset[1]
        missing_start_off = missing_offset[0]
        missing_end_off = missing_offset[1]
        missing_offsets.remove(missing_offset)
        if add_start_off > missing_start_off and add_end_off < missing_end_off:
            # create 2 new tuples:
            # (missing_start_off, add_start_off - 1)
            # (add_end_off + 1, missing_end_off)
            missing_offsets.append((missing_start_off, add_start_off - 1))
            missing_offsets.append((add_end_off + 1, missing_end_off))
        elif add_end_off == missing_end_off:
            # create tuple: (missing_start_off, add_start_off - 1)
            missing_offsets.append((missing_start_off, add_start_off - 1))
        elif add_start_off == missing_start_off:
            # create tuple: (add_end_off + 1, missing_end_off)
            missing_offsets.append((add_end_off + 1, missing_end_off))
        list.sort(missing_offsets)

    missing_file_names = missing_files.keys()
    random.shuffle(missing_file_names)
    for random_file in missing_file_names:
        if random_file in other_thread_data:
            other_thread_offset_ranges = other_thread_data[random_file]
            # remove offsets from missing_files that exist in
            # other_thread_offset_ranges, and update missing_files list using
            # add_missing_offsets() with offsets from other_thread_offset_ranges
            for other_thread_offset_range in other_thread_offset_ranges:
                if other_thread_offset_range in missing_files[random_file]:
                    missing_files[random_file].remove(other_thread_offset_range)
                else:
                    add_missing_offset(missing_files[random_file],
                            other_thread_offset_range)
        if not missing_files[random_file]:
            continue
        # pick any offset of length at most 100 for the file and return
        random_offset_range = random.choice(missing_files[random_file])
        first_offset = random_offset_range[0]
        last_offset = random_offset_range[1]
        offset_len = last_offset - first_offset + 1
        if offset_len <= 100:
            return (random_file, random_offset_range)
        # extract an offset range of length 100 from random_offset_range
        while True:
            #print 'offset try get:', first_offset, last_offset
            first_offset = random.randint(first_offset, last_offset)
            offset_len = last_offset - first_offset + 1
            if offset_len >= 100:
                last_offset -= (offset_len - 100)
                break
        return (random_file, (first_offset, last_offset))
    return None

def thread_func(node_ip, zk_handle, thread_data, active_threads, shared_lock):
    thread_name = threading.current_thread().getName()
    shared_lock.acquire()
    # thread_data should be formatted like this:
    # {'172.16.1.10': ('n1.f1.txt', (0, 200))}
    node_to_get_from = thread_data.keys()[0]
    file_to_get = thread_data[node_to_get_from][0]
    offsets_to_get = thread_data[node_to_get_from][1]
    shared_lock.release()
    file_path = os.path.join(NODE_FILES_DIR, node_ip, file_to_get)
    print 'Thread %s -> %s trying to get file %s and offsets: %s' % (
            thread_name, node_ip, file_to_get, str(offsets_to_get))
    # TODO: establish socket connection and receive bytes to write to file
    # read from /node-files and then update the offset; using version #
    while True:
        current_files = get_current_file_data(node_ip, zk_handle)
        if file_to_get in current_files:
            file_version = current_files[file_to_get][0]
            file_offsets = current_files[file_to_get][1]
            updated_offsets = update_file_offsets(file_offsets, offsets_to_get)
            # update the file's znode under /node-files
            znode_offsets = get_znode_offsets(updated_offsets)
            try:
                zk_handle.set(file_path, bytes(znode_offsets), file_version)
            except kazoo.exceptions.BadVersionError as e:
                continue
            print 'Thread %s -> %s successfully updated file %s with offsets: %s' % (thread_name, node_ip, file_to_get,
                    str(updated_offsets))
            break
        else:
            # TODO: create the file's znode under /node-files
            print 'Thread %s -> %s shouldn\'t be here' % (thread_name, node_ip)
            break
    shared_lock.acquire()
    thread_data.clear()
    active_threads[thread_name] = None
    shared_lock.release()

def main(node_ip):
    zk_handle = KazooClient(hosts='127.0.0.1:2181')
    try:
        zk_handle.start()
    except:
        raise Exception('Could not establish connection to ZK server')

    thread1_data = {}
    thread2_data = {}
    active_threads = {THREAD1: None, THREAD2: None}
    # use shared_lock to read/write active_threads and print to stdou
    shared_lock = threading.Lock()

    while True:
        current_files = get_current_file_data(node_ip, zk_handle)
        missing_files = get_missing_file_data(node_ip, zk_handle)
        nodes_with_missing_files = get_nodes_with_missing_files(node_ip,
                zk_handle, missing_files)
        if not nodes_with_missing_files:
            time.sleep(5)  # wait 5 seconds before checking again
            continue

#        shared_lock.acquire()
#        print '%s -> %s\'s current files: %s' % ('main thread', node_ip,
#                str(current_files))
#        print '%s -> %s\'s missing files: %s' % ('main thread', node_ip,
#                str(missing_files))
#        print '%s -> Nodes that contain the missing files of %s: %s' % (
#                'main thread', node_ip, str(nodes_with_missing_files))
#        shared_lock.release()

        # order the nodes with the missing files based on number of missing
        # files
        nodes_with_missing_files = OrderedDict(sorted(
                nodes_with_missing_files.items(), key=lambda t: len(t[1])))

        print 'active_threads:', str(active_threads)

        shared_lock.acquire()
        if not active_threads[THREAD1] and nodes_with_missing_files.items():
            print 'testdebug1'
            node, missing_files = nodes_with_missing_files.items()[0]
            del nodes_with_missing_files[node]
            random_file, random_offset = get_random_file_offset(missing_files,
                    thread2_data)
            thread1_data[node] = (random_file, random_offset)
            active_threads[THREAD1] = threading.Thread(target=thread_func,
                    name=THREAD1, args=(node_ip, zk_handle, thread1_data,
                    active_threads, shared_lock))
            active_threads[THREAD1].start()
        if not active_threads[THREAD2] and nodes_with_missing_files.items():
            print 'testdebug2'
            node, missing_files = nodes_with_missing_files.items()[0]
            del nodes_with_missing_files[node]
            random_file, random_offset = get_random_file_offset(
                    missing_files, thread1_data)
            thread2_data[node] = (random_file, random_offset)
            active_threads[THREAD2] = threading.Thread(target=thread_func,
                    name=THREAD2, args=(node_ip, zk_handle, thread2_data,
                    active_threads, shared_lock))
            active_threads[THREAD2].start()
        shared_lock.release()

    zk_handle.stop()
    zk_handle.close()

parser = argparse.ArgumentParser(description='node IP')
parser.add_argument('node_ip', nargs=1, type=str, help='node IP')
args = parser.parse_args()
main(args.node_ip[0])
