import argparse
import socket
import os

curr_port = 4950

def main(node_ip):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((node_ip, curr_port))
    while True:
        data, (addr, port) = sock.recvfrom(100)
        print 'rcvd: %s' % data
        file_name, offset = data.split(',')
        # read from file
        try:
            fd = os.open(file_name, os.O_RDONLY)
            os.lseek(fd, int(offset), os.SEEK_SET)
            data = os.read(fd, 1)
            os.close(fd)
            if data == '':
                raise
            # send to (addr, port)
            sock.sendto(data, (addr, port))
            print 'sent: %s' % data
        except:
            continue

parser = argparse.ArgumentParser(description='node IP')
parser.add_argument('node_ip', nargs=1, type=str, help='node IP')
args = parser.parse_args()
main(args.node_ip[0])
