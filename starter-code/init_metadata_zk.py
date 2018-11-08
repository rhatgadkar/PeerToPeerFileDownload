from kazoo.client import KazooClient

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

zk.create('/node-files')
zk.create('/node-files/172.16.1.10')
zk.create('/node-files/172.16.1.10/n0.f1.txt', value='0,200;500,800')
zk.create('/node-files/172.16.1.10/n1.f1.txt', value='0,8')
zk.create('/node-files/172.16.1.11')
zk.create('/node-files/172.16.1.11/n0.f1.txt', value='0,800')
zk.create('/node-files/172.16.1.11/n1.f1.txt', value='0,9')

zk.create('/alive-nodes')
zk.create('/alive-nodes/172.16.1.10', ephemeral=False)
zk.create('/alive-nodes/172.16.1.11', ephemeral=False)

zk.create('/all-files')
zk.create('/all-files/n0.f1.txt', value='0,800')
zk.create('/all-files/n1.f1.txt', value='0,9')

zk.stop()
zk.close()
