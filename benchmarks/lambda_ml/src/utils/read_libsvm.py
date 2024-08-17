def svm_read_problem(data):
	"""
	data: bytes-like object in LIBSVM-format
    return [y, x], y: list, x: list of dictionary
	"""
	prob_y = []
	prob_x = []
	row_ptr = [0]
	col_idx = []
	data = data.decode().split("\n")
	print("#rows in the dataset: {}".format(len(data)))
	for i, line in enumerate(data):
		line = line.split(None, 1)
		# In case an instance with all zero features
		if len(line) == 1: line += ['']
		if len(line) == 0: break
		label, features = line
		prob_y.append(float(label))
		xi = {}
		for e in features.split():
			ind, val = e.split(":")
			xi[int(ind)] = float(val)
		prob_x += [xi]
	print("Label dim: {}, data dim: {}".format(len(prob_y), len(prob_x)))
	return (prob_y, prob_x)
