from collections import Counter
from nltk.corpus import stopwords
from iterable_queue import IterableQueue
import t4k
from twitter_csv_read import TwitterCSVReader
from document_iterator import DocumentIterator
from multiprocessing import Process
from scipy.special import beta as beta_func
import numpy as np

STOPWORDS = set(stopwords.words('english'))
DEFAULT_NUM_TOPICS = 50
DOCUMENTS = 0
WORDS = 1
TOPICS = 2
NUM_PROCS = 12
SMOOTH_MIN = 0.005

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
        alpha=None,
        beta=0.1,
        num_procs=NUM_PROCS,
        read=TwitterCSVReader().read,
        num_docs=None,
        min_frequency=5,
    ):
        """
        Infers model TOT parameters, discovering time-resolved topics from a
        series of documents. Delegates to the global fit function in this 
        module.  Before calling the global fit function, checks whether the 
        dictionary and number of documents per processor is known, and if not
        determines these first.
        """

        return fit(
            dictionary=dictionary,
            files=files,
            dirs=dirs,
            match=match,
            skip=skip,
            batch_size=batch_size,
            num_topics=num_topics,
            alpha=alpha,
            beta=beta,
            num_procs=num_procs,
            read=read,
            num_docs=num_docs,
            min_frequency=min_frequency,
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
    alpha=None,
    beta=0.1,
    num_procs=NUM_PROCS,
    read=TwitterCSVReader().read,
    num_docs=None,
    min_frequency=5,
    num_epochs=100
):

    #np.random.seed(1)

    # If we don't have the number of documents or a dictionary, then
    # run over the full dataset once to accumulate that information.
    if dictionary is None or num_docs is None:
        dictionary, num_docs = (
            construct_dictionary_and_count_documents(
                files=files, dirs=dirs, match=match, skip=skip,
                batch_size=batch_size, num_procs=num_procs, read=read,
                stopwords=STOPWORDS, min_frequency=min_frequency
            ))

    if alpha is None:
        alpha = 1./num_topics

    total_docs = sum(num_docs)
    proc_doc_indices = [sum(num_docs[:i]) for i in range(len(num_docs)+1)]

    m = np.ones((total_docs, num_topics))
    n = np.ones((len(dictionary), num_topics))

    psi = np.ones((2, num_topics))

    for epoch in range(num_epochs):

        # Pre-calculate the denominator in the sum of the probability dist
        n_denom = (n + beta).sum(axis=0) - 1
        B = np.array([beta_func(*psi_vals) for psi_vals in psi])
        denom = n_denom * B

        # The workers should calculate probabilities and then sample, producing
        # updates to m and n.
        updates_queue = IterableQueue()
        for proc_num in range(num_procs):

            # Advance the randomness so children don't all get same seed
            np.random.random()

            doc_iterator = DocumentIterator(
                read=read, files=files, dirs=dirs, match=match, skip=skip,
                batch_size=batch_size,
                fold='%s/%s' % (proc_num, num_procs),
            )
            m_slice = m[proc_doc_indices[proc_num]:proc_doc_indices[proc_num+1]]

            p = Process(
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
        print proc_doc_indices
        for proc_num, m_update, n_update, psi_update in updates_consumer:
            print m
            n += n_update
            start_idx = proc_doc_indices[proc_num]
            stop_idx = proc_doc_indices[proc_num+1]
            m[start_idx : stop_idx] = m_update
            for i in range(num_topics):
                psi_updates[i].extend(psi_update[i])

        # Update psi
        for i in range(num_topics):
            psi[:,i] = fit_psi(psi_update[i])

        print m

    return m, n, psi, dictionary

def fit_psi(samples):
    return 1,1


def construct_dictionary_and_count_documents(
    files=[],
    dirs=[],
    match='',
    skip='$.^',
    batch_size=1000,
    num_procs=NUM_PROCS,
    read=TwitterCSVReader().read,
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
            stopwords,
        )
        p = Process(target=dictionary_worker, args=args)
        p.start()

    # Collect the workers' dictionaries into one.
    worker_dictionary_consumer = worker_dictionary_queue.get_consumer()
    worker_dictionary_queue.close()
    dictionary = t4k.UnigramDictionary()
    for worker_dictionary in worker_dictionary_consumer:
        dictionary.add_dictionary(worker_dictionary)

    # Prune rare words from the dictionary.
    dictionary.prune(min_frequency)

    # Get the number of documents for each process
    worker_num_docs_consumer = worker_num_docs_queue.get_consumer()
    worker_num_docs_queue.close()
    num_docs = [count for proc_num, count in sorted(worker_num_docs_consumer)]

    # Return the completed, pruned dictionary.
    return dictionary, num_docs


def dictionary_worker(
    proc_num, 
    documents_iterator,
    dictionary_queue,
    num_docs_queue,
    stopwords=set()
):
    dictionary = t4k.UnigramDictionary()
    num_docs = 0
    for timestamp, tokens in documents_iterator:
        dictionary.update([t for t in tokens if t not in stopwords])
        num_docs += 1

    dictionary_queue.put(dictionary)
    dictionary_queue.close()

    num_docs_queue.put((proc_num, num_docs))
    num_docs_queue.close()


def worker(
    proc_num, documents, dictionary, num_topics, 
    alpha, beta, psi, n, m, denom, updates_producer
):

    # new m and n matrices
    new_n = np.zeros(n.shape)
    new_m = np.zeros(m.shape)
    psi_update = [[] for i in range(num_topics)]

    for doc_idx, (timestamp, document) in enumerate(documents):

        counted = Counter(document)
        counts = [
            (dictionary.get_id(word), count)
            for word, count in counted.iteritems()
        ]

        #print '\t*** document ***:', doc_idx

        for word_idx, count in counts:

            # Calculate multinomial probabilities over topics for this word
            # in this document
            
            #print 'denom:', repr(denom)
            P = (
                (m[doc_idx] + alpha - 1) 
                * (n[word_idx] + beta - 1)
                * (1-timestamp)**(psi[0]-1) 
                * timestamp**(psi[1]-1)
                / denom
            )

            #print 'word:', dictionary.get_token(word_idx)
            #print 'raw P', P

            # If any values are negative, shift, smooth, and normalize.
            min_val = np.min(P)
            if min_val < 0:
                # Shift so that smallest value becomes zero; all other values
                # increased by the same absolute amount
                P -= min_val
                #print 'shifted P', P

                # Normalize to a length-1 vector
                P = P / np.linalg.norm(P, ord=2)   
                #print 'pre-smooth-norm P', P

                # Smooth -- make small values be at least ``SMOOTH_MIN``, then
                # re-normalize
                P = vec_max(P, SMOOTH_MIN)
                #print 'smooth P', P

            # Normalize to a sum-to-1 vector
            P = P / np.linalg.norm(P, ord=1)   
            #print 'norm P', P

            # Sample from the multinomial distribution, and update m and n.
            sample = np.random.multinomial(count, P)
            #print 'count', count
            #print 'sample', sample
            #print '\n\n'

            new_n[word_idx] += sample
            new_m[doc_idx] += sample
            for topic, count in enumerate(sample):
                psi_update[topic].extend([timestamp]*count)

    #print 'new_m:', repr(new_m)
    #print 'new_n:', repr(new_n)
    #print 'psi_update:', repr(psi_update)
    #t4k.out('.')

    updates_producer.put((proc_num, new_m, new_n, psi_update))
    updates_producer.close()







