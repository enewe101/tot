from unigram_dictionary import UnigramDictionary
import time
import numpy as np
import operator
from collections import Counter
from nltk.corpus import stopwords
from scipy.special import beta as beta_func
from iterable_queue import IterableQueue
from multiprocessing import Process
import multiprocessing as mp
from document_iterator import DocumentIterator
from beta_estimator import estimate_beta

STOPWORDS = set(stopwords.words('english'))
DEFAULT_NUM_TOPICS = 50
DOCUMENTS = 0
WORDS = 1
TOPICS = 2
NUM_PROCS = 12
SMOOTH_MIN = 0.005
EPS = 1e-1

# TODO: make n and m only two-dimensional, since we always discard their
# respective inactive dimensions.
# Test the update calculations


vec_max = np.vectorize(max)

class TopicsOverTimeModel(object):


    def __init__(self, dictionary=None):
        self.dictionary=dictionary


    def fit(
        self,
        files=[],
        dirs=[],
        match='',
        skip='$.^',
        batch_size=1000,
        num_topics=DEFAULT_NUM_TOPICS,
        time_range=None,
        alpha=None,
        beta=0.1,
        num_procs=NUM_PROCS,
        read=None,
        num_docs=None,
        min_frequency=5,
        num_epochs=100
    ):
        """
        Infers model TOT parameters, discovering time-resolved topics from a
        series of documents. Delegates to the global fit function in this 
        module.  Before calling the global fit function, checks whether the 
        dictionary and number of documents per processor is known, and if not
        determines these first.
        """

        return fit(
            dictionary=self.dictionary,
            files=files,
            dirs=dirs,
            match=match,
            skip=skip,
            batch_size=batch_size,
            num_topics=num_topics,
            time_range=None,
            alpha=alpha,
            beta=beta,
            num_procs=num_procs,
            read=read,
            num_docs=num_docs,
            min_frequency=min_frequency,
            num_epochs=num_epochs
        )


    def save(self, path):
        pass


    def load(self, path):
        pass


def fit(
    dictionary=None,
    files=[],
    dirs=[],
    match='',
    skip='$.^',
    batch_size=1000,
    num_topics=DEFAULT_NUM_TOPICS,
    time_range=None,
    alpha=None,
    beta=0.1,
    num_procs=NUM_PROCS,
    read=None,
    num_docs=None,
    min_frequency=5,
    num_epochs=100
):

    # If we don't have the number of documents or a dictionary, then
    # run over the full dataset once to accumulate that information.
    if dictionary is None or num_docs is None or time_range is None:
        dictionary, num_docs, found_time_range = (
            get_corpus_stats(
                files=files, dirs=dirs, match=match, skip=skip,
                batch_size=batch_size, num_procs=num_procs, read=read,
                stopwords=STOPWORDS, min_frequency=min_frequency
            ))

    if time_range is None:
        time_range = found_time_range

    if alpha is None:
        alpha = 1.

    total_docs = sum(num_docs)
    proc_doc_indices = [sum(num_docs[:i]) for i in range(len(num_docs)+1)]

    m = np.ones((total_docs, num_topics))
    n = np.ones((len(dictionary), num_topics))

    psi = np.ones((num_topics, 2))

    #TODO: move worker creation outside of the epoch -- keep same worker pool
    # between epochs.  Workers can receive updates about m and n etc. over the
    # queue.
    for epoch in range(num_epochs):

        # Show progress
        print(float(epoch)/num_epochs * 100)

        # Pre-calculate the denominator in the sum of the probability dist
        n_denom = (n + beta).sum(axis=0) - 1
        B = np.array([beta_func(*psi_vals) for psi_vals in psi])
        denom = n_denom * B

        # The workers should calculate probabilities and then sample, producing
        # updates to m and n.
        updates_queue = IterableQueue()
        ctx = mp.get_context("spawn")
        for proc_num in range(num_procs):

            # Advance the randomness so children don't all get same seed
            np.random.random()

            doc_iterator = DocumentIterator(
                read=read, files=files, dirs=dirs, match=match, skip=skip,
                batch_size=batch_size,
                fold='%s/%s' % (proc_num, num_procs),
            )
            m_slice = m[proc_doc_indices[proc_num]:proc_doc_indices[proc_num+1]]

            p = ctx.Process(
                target=worker,
                args=(
                    proc_num, doc_iterator, dictionary, num_topics,
                    alpha, beta, psi, n, m_slice, denom, 
                    updates_queue.get_producer()
                )
            )
            p.start()

        updates_consumer = updates_queue.get_consumer()
        updates_queue.close()

        # Update m, n, and psi
        n = np.zeros((len(dictionary), num_topics))
        m = np.zeros((total_docs, num_topics))
        psi_updates = [[] for i in range(num_topics)]
        for proc_num, m_update, n_update, psi_update in updates_consumer:
            n += n_update
            start_idx = proc_doc_indices[proc_num]
            stop_idx = proc_doc_indices[proc_num+1]
            m[start_idx : stop_idx] = m_update
            for i in range(num_topics):
                psi_updates[i].extend(psi_update[i])

        # Update psi
        for i in range(num_topics):
            psi[i] = fit_psi(psi_updates[i])

    return m, n, psi, dictionary

def fit_psi(samples):
    alpha, beta = estimate_beta(samples)
    return alpha, beta

def get_corpus_stats(
    files=[],
    dirs=[],
    match='',
    skip='$.^',
    batch_size=1000,
    num_procs=NUM_PROCS,
    read=None,
    stopwords=STOPWORDS,
    min_frequency=5
):
    """
    Build a dictionary by running through the dataset fully.
    prune back according to min_frequency.  Ignore stopwords given.
    This dictionary facilitates the conversion between tokens and integers.
    """

    # Start meany workers.  Each will make a dictionary over a subset of the
    # documents.  They return their dictionaries over a queue.
    worker_dictionary_queue = IterableQueue()
    worker_num_docs_queue = IterableQueue()
    worker_time_range_queue = IterableQueue()
    ctx = mp.get_context('spawn')
    for proc_num in range(num_procs):
        doc_iterator = DocumentIterator(
            read=read, files=files, dirs=dirs, match=match, skip=skip, 
            batch_size=batch_size,
            fold='%s/%s' % (proc_num, num_procs),
        )
        args = (
            proc_num,
            doc_iterator,
            worker_dictionary_queue.get_producer(),
            worker_num_docs_queue.get_producer(),
            worker_time_range_queue.get_producer(),
            stopwords,
        )
        p = ctx.Process(target=dictionary_worker, args=args)
        p.start()

    # Collect the workers' dictionaries into one.
    worker_dictionary_consumer = worker_dictionary_queue.get_consumer()
    worker_dictionary_queue.close()
    dictionary = UnigramDictionary()
    for worker_dictionary in worker_dictionary_consumer:
        dictionary.add_dictionary(worker_dictionary)

    # Prune rare words from the dictionary.
    dictionary.prune(min_frequency)

    # Get the number of documents for each process
    worker_num_docs_consumer = worker_num_docs_queue.get_consumer()
    worker_num_docs_queue.close()
    num_docs = [count for proc_num, count in sorted(worker_num_docs_consumer)]

    # Get time range for all documents
    worker_time_range_consumer = worker_time_range_queue.get_consumer()
    worker_time_range_queue.close()

    minimum_t = 999999999999
    maximum_t = 0
    for min_time, max_time in worker_time_range_consumer:
        minimum_t = min(min_time,minimum_t)
        maximum_t = max(max_time,maximum_t)

    #buffering with 1% on both sides to ensure a nonzero chance for each document
    time_difference = time_range[1] - time_range[0]
    wiggle_room = time_difference/100
    time_range = (minimum_t - wiggle_room, maximum_t + wiggle_room)

    # Return the completed, pruned dictionary, and time range.
    return dictionary, num_docs, time_range


def dictionary_worker(
    proc_num, 
    documents_iterator,
    dictionary_queue,
    num_docs_queue,
    time_range_queue,
    stopwords=set()
):
    dictionary = UnigramDictionary()
    min_time = 999999999999
    max_time = 0
    num_docs = 0
    for timestamp, tokens in documents_iterator:
        dictionary.update([t for t in tokens if t not in stopwords])
        min_time = min(min_time, timestamp)
        max_time = max(max_time, timestamp)
        num_docs += 1

    dictionary_queue.put(dictionary)
    dictionary_queue.close()

    time_range_queue.put((min_time,max_time))
    time_range.queue.close()

    num_docs_queue.put((proc_num, num_docs))
    num_docs_queue.close()


def worker(
    proc_num, documents, dictionary, num_topics, time_range,
    alpha, beta, psi, n, m, denom, updates_producer
):

    # new m and n matrices
    new_n = np.zeros(n.shape)
    new_m = np.zeros(m.shape)
    psi_update = [[] for i in range(num_topics)]

    for doc_idx, (timestamp, document) in enumerate(documents):

        #normalize timestamp
        timestamp = np.divide((timestamp - time_range[0], time_range[1] - time_range[0]))

        counted = Counter(document)
        counts = [
            (dictionary.get_id(word), count)
            for word, count in counted.items()
            if dictionary.get_id(word) > 0
        ]

        for word_idx, count in counts:

            # Calculate multinomial probabilities over topics for this word
            # in this document
            P = (
                (m[doc_idx] + alpha - 1) 
                * (n[word_idx] + beta - 1)
                * (timestamp)**(psi[:,0]-1) 
                * (1-timestamp)**(psi[:,1]-1)
                / denom
            )

            # If any values are negative, shift, smooth, and normalize.
            min_val = np.min(P)
            if min_val < 0:
                # Shift so that smallest value becomes zero; all other values
                # increased by the same absolute amount
                P -= min_val

                # Normalize to a length-1 vector
                P = P / np.linalg.norm(P, ord=2)   

                # Smooth -- make small values be at least ``SMOOTH_MIN``, then
                # re-normalize
                P = vec_max(P, SMOOTH_MIN)

            # Normalize to a sum-to-1 vector
            P = P / np.linalg.norm(P, ord=1)   

            # Sample from the multinomial distribution, and update m and n.
            sample = np.random.multinomial(count, P)
            new_n[word_idx] += sample
            new_m[doc_idx] += sample
            for topic, count in enumerate(sample):
                psi_update[topic].extend([timestamp]*count)

    updates_producer.put((proc_num, new_m, new_n, psi_update))
    updates_producer.close()

def print_top_words_per_topic(n,psi,dictionary,nb_words):
     n_transposed = np.transpose(n)
     all_words = {}
     for idx, topic in enumerate(n_transposed):
         max_words = []
         for iteration in range(nb_words):
             index, value = max(enumerate(topic), key=operator.itemgetter(1))
             topic[index] = -1
             max_words.append((index,value))
         print("topic {0}: \n psi values: {1} \n top words:".format(str(idx), psi[idx]))
         for jdx, word in enumerate(max_words):
             actual_word = dictionary.get_token(word[0])
             max_words[jdx] = actual_word
             print(actual_word)
         all_words[idx] = max_words
     return all_words
 






