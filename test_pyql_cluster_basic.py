import test_pyql_cluster

def main(count):
    c = test_pyql_cluster.cluster(debug=True)
    c.init_cluster()
    c.auth_setup()
    for _ in range(count):
        c.expand_cluster()
if __name__ == '__main__':
    import sys
    assert len(sys.argv) == 2, "expected 1 argument for # number of nodes"
    count = int(sys.argv[1])
    main(count)
