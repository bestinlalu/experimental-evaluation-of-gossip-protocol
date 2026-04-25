python clear_database.py
python start_network_faulty_peer.py -strategy PULL -nodes 4
python test_report_1.py -interval 5 -uid-count 1
python stop_network.py