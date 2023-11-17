import unittest

from cache import StringCache


class TestingStringCacheCls(unittest.TestCase):
    test_arr = ['unqiue_1', 'other_1', 'unqiue_1']

    def test_functional(self):
        str_cache = StringCache()
        for test_item in TestingStringCacheCls.test_arr:
            str_cache.add(test_item)

        print(str_cache.index)
        print(str_cache.dictionary)
        print(str_cache.reverse_dictionary)
        print(str_cache.get(1))

    def test_restore_cache(self):
        str_cache = StringCache()

        data_1 = bytes('value_1,value_2', encoding='UTF-8')
        str_cache.append_from_bytes(data_1)
        data_2 = bytes('value_3', encoding='UTF-8')
        str_cache.append_from_bytes(data_2)

        print(str_cache.index)
        print(str_cache.dictionary)
        print(str_cache.reverse_dictionary)
        print(str_cache.get(1))


if __name__ == '__main__':
    unittest.main()
