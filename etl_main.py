from time import time


def timer(func):
	def wrap_func(*args, **kwargs):
		t1 = time()
		result = func(*args, **kwargs)
		t2 = time()
		print(f'Function {func.__name__!r} executed in {(t2 - t1):.4f}s')
		return result

	return wrap_func


@timer
def read_unique_tracks(file_path: str) -> dict:
	"""
	Input dim file structure:
	performance_id<SEP>track_id<SEP>artist_name<SEP>track_name<LF>
	:param file_path: File path
	:return: Dict {"track_id":["artist_name", "track_name"]}
	"""
	with open(file_path, "rb") as f:
		data = f.read()
		data_list = [line.split(b"<SEP>") for line in data.split(b"\n")]
		data_dict = {}
		for item in data_list:
			if len(item) == 4:
				data_dict[item[1]] = [item[2], item[3]]
	return data_dict


@timer
def read_transform_sample_file(file_path:str, artist_track: dict) -> dict:
	"""
	Input fact file structure:
	user_id<SEP>track_id<SEP>listening_timestamp<LF>
	:param file_path: File path
	:param artist_track: Dict from read_unique_tracks
	:return: Dict {"track_id":["count_of_listens", "artist_name", "track_name"]}
	"""
	with open(file_path, "rb") as f:
		agg = {}
		line = f.readline()
		while line:
			user_id, track_id, event_timestamp = line.split(b"<SEP>")
			event_timestamp = event_timestamp[:-1]
			if track_id in agg.keys():
				agg[track_id][0] += 1
			else:
				agg[track_id] = [1, artist_track[track_id][0], artist_track[track_id][1]]
			line = f.readline()
	return agg


@timer
def top_n_from_dict(dict, key, reverse, n):
	print(sorted(dict.items(), key=key, reverse=reverse)[:n])


path = "src/unique_tracks.txt"
path2 = "src/triplets_sample_20p.txt"

artist_track_dict = read_unique_tracks(path)

aggregated = read_transform_sample_file(path2, artist_track_dict)

# COUNT LISTENS PER ARTIST (SUM TRACK LISTEN COUNTS PER ARTIST)

top_artists = {}

for values in aggregated.values():
	if values[1] in top_artists.keys():
		top_artists[values[1]] += values[0]
	else:
		top_artists[values[1]] = values[0]

top_n_from_dict(top_artists, lambda x: x[1], True, 5)


