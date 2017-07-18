
import numpy as np


# def normalize_timestamp(timestamps):
# 	normalized = []
# 	start = min(timestamps)
# 	end = max(timestamps)
# 	time_interval = end - start

# 	for t in timestamps:
# 		normalized.append(np.divide(np.array(timestamps) - start, time_interval))

# 	return normalized
		

def estimate_beta(timestamps_normalized):
	mean = np.mean(timestamps_normalized)
	variance = np.divide(np.sum(np.square(np.array(timestamps_normalized) - mean)), len(np.array(timestamps_normalized) - 1))

	mean_times_one_minus_mean_over_variance = np.divide(mean*(1-mean), variance)
	alpha = mean*(mean_times_one_minus_mean_over_variance - 1)
	beta = (1-mean)*(mean_times_one_minus_mean_over_variance - 1)

	return alpha, beta
