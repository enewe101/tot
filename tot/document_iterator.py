import os
import re
# import t4k
import file_utils


def do_open(path):
    return [open(path)]

class DocumentIterator(object):
    """
    This class supports iterating over all of the documents (files) that are
    found in a given list of files and/or under given directories.

    If recurse is True (default False), it will iterate over files in
    subdirectories too.

    This supports iterating over non-overlapping subsets of the given set of
    files, by using the folds argument.  It should take the form ``a/b``, which
    means this iterator should yield the ``a``th subset of ``b`` total subsets.
    Note that ``a`` is zero-indexed, so a should be in [0,b-1].

    The main business of the iterator is managing which files are to be
    iterated.  It relies on a reader to actually read the file from disk into
    an arbitrary python object.  So the iterator actually yields the python
    objects that result from applying the reader to the file paths.  The default
    reader simply yields file objects open for reading.  To support the
    possibility that there are multiple ``documents`` per file, the file reader
    should return a list of python objects even if there is just one.

    The iterator will read a large batch of files into memory, before yielding 
    the first one.  Once it reads a batch, it will yield until the batch is
    empty, then read another large batch.
    """

    def __init__(
        self, read=do_open, files=[], dirs=[], match='', skip='$.^', 
        fold='0/1', batch_size=10000, skip_err=False, use_hash=False
    ):
        """
        Note that the default for match will match everything, and the 
        default for skip will match nothing.
        """
        self.fold, self.num_folds = [int(s) for s in fold.split('/')]
        self.skip = self.compile_regexes(skip)
        self.match = self.compile_regexes(match)
        self.batch_size = batch_size
        self.read = read
        self.skip_err = skip_err
        self.use_hash = use_hash
        self.files = self.filter_files(files, dirs)


    def __len__(self):
        return len(self.files)


    def __iter__(self):
        ptr = 0
        while True:

            # Read a batch of documents
            documents = []
            for f in self.files[ptr:ptr+self.batch_size]:
                try:
                    documents.extend(self.read(f))
                except IOError:
                    if not self.skip_err:
                        raise

            # If there are none left, stop iteration
            if len(documents) == 0:
                raise StopIteration()

            # Yield the documents until the batch is done
            for document in documents:
                yield document

            # Move the pointer along to the next batch, and loop 
            ptr += self.batch_size


    def compile_regexes(self, regex):
        if isinstance(regex, str):
            return re.compile(regex)
        else:
            return re.compile('(' + ')|('.join(skips) + ')')


    def filter_files(self, files, dirs):

        # Generally we expect lists, but single file or dirnames are handled.
        if isinstance(files, str):
            files = [files]
        if isinstance(dirs, str):
            dirs = [dirs]

        # Absolutize paths, and filter files against ``self.skip`` and 
        # ``self.match``
        filtered_files = []
        filtered_files.extend(self.filter_filelist(files))
        for directory in dirs:
            try:
                filtered_files.extend(self.filter_filelist(file_utils.ls(directory)))
            except OSError:
                if not self.skip_err:
                    raise

        # Take only the files that belong to this bin.  Sort first for
        # consistency.  We'll either bin files based on the hash of the
        # filename (by relying on t4k's inbin function, or we'll bin by taking
        # every nth file to be in this bin 
        filtered_files.sort()
        # if self.use_hash:
        #     is_in_bin = lambda i,f: t4k.inbin(f,self.num_folds, self.fold)
        # else:
        is_in_bin = lambda i,f: i % self.num_folds == self.fold
        filtered_files = [
            f for i, f in enumerate(filtered_files) if is_in_bin(i,f)
        ]

        return filtered_files


    def filter_filelist(self, files):
        return (
            f for f in (os.path.abspath(q) for q in files)
            if self.match.search(f) and not self.skip.search(f)
        )


