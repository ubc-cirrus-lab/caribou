import memcache

def clear_all(client):
    client.flust_all()
    return True