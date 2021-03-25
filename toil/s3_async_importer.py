"""
s3_async_importer.py

A simple script to import large files to AWS s3 with Toil, asynchronously.

Adapted from: https://github.com/vgteam/toil-vg under the Apache v2.0 License:
https://github.com/vgteam/toil-vg/blob/master/LICENSE

Created by wlgao on 03/25/2021
"""

import logging
import timeit
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

from toil.common import Toil
from toil.fileStores import FileID
from toil.job import Job

logger = logging.getLogger(__name__)

job_store = 'aws:us-west-2:wlgao-async-import-test'
files_to_import = [
    # files from the Toil public s3 datasets
    'http://toil-datasets.s3.amazonaws.com/ENCODE_data.zip',  # 67.6 MB
    'http://toil-datasets.s3.amazonaws.com/wdl_templates.zip',  # 2.2 MB
    'http://toil-datasets.s3.amazonaws.com/GATK_data.zip',  # 304.2 MB

]


class AsyncImporter:
    """
    Asynchronously import files to a AWS s3 bucket using Toil.
    """

    def __init__(self, toil, max_threads: int = 8):
        """
        :param toil: A toil or toil fileStore instance that has the method `importFile`.
        :param max_threads: The maximum number of workers to use during file import.
        """
        self.toil = toil
        self.max_threads = max_threads

        self.files = []

    def add(self, file_path: str) -> None:
        """ Add a file URL to be imported."""
        self.files.append(file_path)

    def load(self) -> list:
        """
        Import all files asynchronously.
        :return: A list of local fileIDs.
        """
        # logger.info(f'Importing {len(self.files)} input files into Toil.')
        results = []

        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = []

            for file in self.files:
                logger.info(f'Importing "{file}"...')
                futures.append(executor.submit(self.toil.importFile, file))

            for future in as_completed(futures):
                file_id = future.result()
                assert isinstance(file_id, FileID)

                results.append(file_id)
                logger.info(f'File ID "{file_id}" (with size="{file_id.size}") imported successfully.')

        assert len(self.files) == len(results), \
            f'Incorrect number of files imported: {len(self.files)} != {len(results)}'

        return results


def main():
    """ Program entry."""
    options = Job.Runner.getDefaultOptions(jobStore=job_store)
    options.clean = 'never'

    with Toil(options) as toil:
        start_time = timeit.default_timer()

        importer = AsyncImporter(toil=toil)
        for file in files_to_import:
            importer.add(file)

        results = importer.load()

        logger.info('Imported {} input files into Toil in {} seconds.'.format(
            len(results), timeit.default_timer() - start_time))


if __name__ == "__main__":
    main()
