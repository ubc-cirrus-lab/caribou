

def load_tags(file_name, max_id):
    tags = {}
    tag_file = open(file_name)
    count = 0
    for line in tag_file.readlines():
        splits = line.split()
        if len(splits) >= 2:
            id = int(splits[0])
            tags[id] = splits[1]
            count += 1
        elif len(splits) == 1:
            id = int(splits[0])
            tags[id] = "none"
            count += 1
        else:
            print("wrong line: {}".format(splits))
    tag_file.close()
    print("there are {} valid instances in tag file".format(count))
    return tags


def convert_yfcc100m(in_name, out_name, tags):
    in_file = open(in_name, "r")
    out_file = open(out_name, "w")

    count = 0
    out_count = 0
    while True:
        line = in_file.readline()
        if not line:
            break
        splits = line.split()
        if len(splits) == 4098:
            id = int(splits[0])
            if id in tags:
                tag = tags[id]
                out_str = tag + " " + " ".join(x for x in splits[2:])
                out_file.write(out_str)
                out_file.write('\n')
                out_count += 1
            else:
                print("cannot find id {} in tag file".format(id))
            count += 1

    in_file.close()
    out_file.close()

    print("file {} has {} instances, out file {} has {} instances"
          .format(in_name, count, out_name, out_count))


if __name__ == "__main__":
    max_id = 14068203843
    tag_file = "yfcc100m_autotags"
    tags = load_tags(tag_file, max_id + 1)
    start_index = 4
    end_index = 99
    for i in range(start_index, end_index):
        data_file = "YFCC100M_hybridCNN_gmean_fc6_{}.txt".format(i)
        out_file = "YFCC100M_hybridCNN_gmean_fc6_{}_tag".format(i)
        convert_yfcc100m(data_file, out_file, tags)
