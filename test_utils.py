import unittest


class Testing(unittest.TestCase):
    def test_string(self):
        a = 'some'
        b = 'some'
        self.assertEqual(a, b)

    def test_boolean(self):
        a = True
        b = True
        self.assertEqual(a, b)

    def test_lookup(self):
        val = 255
        val = val << 24
        val = 0xff000000
        print(val)



if __name__ == '__main__':
    unittest.main()
