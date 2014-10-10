"""Represents a writer that returns the result within the response."""

import os
import csv

import mr.config.result
import mr.result_writers.base
import mr.compat


class FileResultWriter(mr.result_writers.base.BaseResultWriter):
    def __init__(self, *args, **kwargs):
        super(FileResultWriter, self).__init__(*args, **kwargs)
        
        root_path = mr.config.result.FILE_WRITER_OUTPUT_DIRECTORY
        if os.path.exists(root_path) is False:
            os.makedirs(root_path)

        self.__root_path = root_path

    def render(self, request_id, result_pair_gen):
        if mr.config.result.FILE_WRITER_WRITE_AS_DIRECTORY is True:
            return self.__write_as_directory(request_id, result_pair_gen)
        else:
            return self.__write_as_file(request_id, result_pair_gen)

    def __write_as_directory(self, request_id, result_pair_gen):
        path = os.path.join(self.__root_path, request_id)

        os.mkdir(path)

        for (k, v) in result_pair_gen:
            # Even though the key and value have to be strings we don't assert 
            # it, but rather do a JIT conversion. We accomodate the situation 
            # where the amount of data that an object needs to represent the 
            # value is considerably less than the number of bytes that'll 
            # actually be in the final string.

            filepath = os.path.join(path, str(k))

            with open(filepath, 'wb') as f:
                f.write(str(v))

    def __write_as_file(self, request_id, result_pair_gen):
        filepath = os.path.join(self.__root_path, request_id)

        with open(filepath, 'wb') as f:
            cf = csv.writer(
                    f, 
                    delimiter=' ',
                    quoting=csv.QUOTE_NONE)

            cf.writerows(result_pair_gen)
