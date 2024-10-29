# Thanks to SK lee who provided the skeleton code

import math, sys
import pandas as pd
import numpy as np
from tqdm import tqdm


class Node:
    def __init__(self, leaf=False):
        self.keys = []
        self.parent = None
        self.children = []
        self.leaf = leaf


class BTree:
    def __init__(self, t):
        '''
        # create a instance of the Class of a B-Tree
        # t : the minimum degree t
        # (the max num of keys is 2*t -1, the min num of keys is t-1)
        '''
        self.root = Node(True)
        self.t = t


    # B-Tree-Split-Child
    def split_child(self, x, i): 
        '''
        # split the node x's i-th child that is full
        # x: the current node
        # i: the index of the node x's child to be split
        # return: None
        '''
        t = self.t
        y = x.children[i] # the node to be split
        z = Node(y.leaf) # new node
        z.parent = x
        
        # insert z to the children of x
        x.children.insert(i+1, z)
        x.keys.insert(i, y.keys[t-1])

        # split
        z.keys = y.keys[t:]
        y.keys = y.keys[:t-1]
        if not y.leaf:
            z.children = y.children[t:]
            y.children = y.children[:t]
            for child in z.children:
                child.parent = z

    # B-Tree-Insert
    def insert(self, k):
        '''
        # insert the key k into the B-Tree
        # return: None
        '''
        root = self.root
        # Case 1: if the root is full
        if len(root.keys) == 2 * self.t - 1:
            # create a new root node
            node = Node()
            node.children.append(root)
            self.root = node
            root.parent = node
            self.split_child(node, 0)
            self.insert_key(node, k)
        # Case 2: if the root is not full
        else:
            self.insert_key(root, k)

    # B-Tree-Insert-Nonfull
    def insert_key(self, x, k):
        '''
        # insert the key k into node x
        # return: None
        '''
        # Case 1: if the node x is leaf
        if x.leaf:
            x.keys.append(k)
            x.keys.sort()
        # Case 2: if the node x is an internal node
        else:
            i = len(x.keys) - 1
            while i >= 0 and k[0] < x.keys[i][0]:
                i -= 1
            i += 1
            # if the child is full, split the child and insert the key
            if len(x.children[i].keys) == 2 * self.t - 1:
                self.split_child(x, i)
                if k[0] > x.keys[i][0]:
                    i += 1
            self.insert_key(x.children[i], k)


    # B-Tree-Search
    def search_key(self, x, key):
        '''
        # search for the key in node x
        # return: the node x that contains the key, the index of the key if the key is in the B-tree
        '''
        # find the node x that contains the key
        i = 0
        while i < len(x.keys) and key > x.keys[i][0]:
            i += 1

        # return x and i if the key is in the tree
        if i < len(x.keys) and key == x.keys[i][0]:
            return x, i
        elif x.leaf:
            return None, None
        # search recursively
        else:
            return self.search_key(x.children[i], key)


    def delete(self, k):
        '''
        # delete the key k from the B-tree
        # return: None
        '''
        # search node to delete
        node, i = self.search_key(self.root, k)

        if node is None:
            return None
        
        if node.leaf:
            self.delete_leaf_node(node, i)
        else:
            self.delete_internal_node(node, i)

    
    def delete_leaf_node(self, x, i):
        '''
        # delete the key in a leaf node
        '''
        # delete the key and fix the tree when the number of keys is smaller than t-1
        x.keys.pop(i)
        if len(x.keys) < self.t - 1:
            self.check_smaller_than_t(x)

    def delete_internal_node(self, x, i):
        '''
        # delete the key in an internal node
        '''
        # find the predecessor and replace
        pred = self.find_predecessor(x, i)
        x.keys[i] = pred.keys.pop()
        # fix the tree
        if len(pred.keys) < self.t - 1:
            self.check_smaller_than_t(pred)

    def check_smaller_than_t(self, x):
        parent = x.parent
        if parent is None:
            return

        curr_node_index = parent.children.index(x)
        left_sibling = parent.children[curr_node_index - 1] if curr_node_index > 0 else None
        right_sibling = parent.children[curr_node_index + 1] if curr_node_index < len(parent.children) - 1 else None

        # if the left sibling has more than t-1 keys, borrow from the left sibling
        if left_sibling and len(left_sibling.keys) > self.t - 1:
            self.borrow_sibling(parent, curr_node_index - 1, curr_node_index)
        # if the right sibling has more than t-1 keys, borrow from the right sibling
        elif right_sibling and len(right_sibling.keys) > self.t - 1:
            self.borrow_sibling(parent, curr_node_index + 1, curr_node_index)
        # if both siblings have a minimum number of keys,
        # borrow a key from the parent node and merge the node with its sibling
        else:
            if left_sibling:
                self.merge_sibling(parent, curr_node_index - 1, curr_node_index)
            elif right_sibling:
                self.merge_sibling(parent, curr_node_index, curr_node_index + 1)
        
        # if the parent node has no keys, make the child node the root
        if parent == self.root and len(parent.keys) == 0:
            self.root = parent.children[0]
            self.root.parent = None
        # if the parent node has fewer than t-1 keys, fix the parent node
        else: 
            if len(parent.keys) < self.t - 1:
                self.check_smaller_than_t(parent)

    def find_predecessor(self, x, i):
        node = x.children[i]
        while not node.leaf:
            node = node.children[-1]
        return node

    def borrow_sibling(self, x, i, j):
        curr_node = x.children[j]
        sibling_node = x.children[i]

        # borrow a key from the left sibling
        if i < j:
            curr_node.keys.insert(0, x.keys[i])
            x.keys[i] = sibling_node.keys.pop()
            if not sibling_node.leaf:
                child = sibling_node.children.pop()
                curr_node.children.insert(0, child)
                child.parent = curr_node
        # borrow a key from the right sibling
        else:
            curr_node.keys.append(x.keys[j])
            x.keys[j] = sibling_node.keys.pop(0)
            if not sibling_node.leaf:
                child = sibling_node.children.pop(0)
                curr_node.children.append(child)
                child.parent = curr_node

    def merge_sibling(self, x, i, j):
        left_node = x.children[i]
        right_node = x.children[j]

        # merge the left node and the parent's key
        left_node.keys.append(x.keys[i])
        left_node.keys.extend(right_node.keys)
        if not right_node.leaf:
            left_node.children.extend(right_node.children)
            for child in right_node.children:
                child.parent = left_node
        
        # delete the parent's key and the right node
        x.keys.pop(i)
        x.children.pop(j)


    # for printing the statistic of the resulting B-tree
    def traverse_key(self, x, level=0, level_counts=None):
        '''
        # run BFS on the B-tree to count the number of keys at every level
        # return: level_counts
        '''
        if level_counts is None:
            level_counts = {}

        if x:
            # counting the number of keys at the current level
            if level in level_counts:
                level_counts[level] += len(x.keys)
            else:
                level_counts[level] = len(x.keys)

            # recursively call the traverse_key() for further traverse
            for child in x.children:
                self.traverse_key(child, level + 1, level_counts)

        return level_counts

# Btree Class done


def get_file():
    '''
    # read an input file (.csv) with its name
    '''
    file_name = (input("Enter the file name you want to insert or delete ▷ (e.g., insert1 or delete1_50 or delete1_90 or ...) "))

    while True:
        try:
            file = pd.read_csv('inputs/'+file_name+'.csv',
                               delimiter='\t', names=['key', 'value'])
            return file
        except FileNotFoundError:
            print("File does not exist.")
            file_name = (input("Enter the file name again. ▷ "))


def insertion_test(B, file):
    '''
    #   read all keys and values from the file and insert them into the B-tree
    #   B   : an empty B-tree
    #   file: a csv file that contains keys to be inserted
    #   return: the resulting B-tree
    '''

    file_key = file['key']
    file_value = file['value']

    print('===============================')
    print('[ Insertion start ]')

    for i in tqdm(range(len(file_key))): # tqdm shows the insertion progress and the elapsed time
        B.insert([file_key[i], file_value[i]])

    print('[ Insertion complete ]')
    print('===============================')
    print()

    return B


def deletion_test(B, delete_file):
    '''
    #   read all keys and values from the file and delete them from the B-tree
    #   B   : the current B-tree
    #   file: a csv file that contains keys to be deleted
    #   return: the resulting B-tree
    '''

    delete_key = delete_file['key']

    print('===============================')
    print('[ Deletion start ]')

    for i in tqdm(range(len(delete_key))):
        B.delete(delete_key[i])

    print('[ Deletion complete ]')
    print('===============================')
    print()

    return B


def print_statistic(B):
    '''
    # print the information about the current B-tree
    # the number of keys at each level
    # the total number of keys in the B-tree
    '''
    print('===============================')
    print('[ Print statistic of tree ]')

    level_counts = B.traverse_key(B.root)

    for level, counts in level_counts.items():
        if level == 0:
            print(f'Level {level} (root): Key Count = {counts}')
        else:
            print(f'Level {level}: Key Count = {counts}')
    print('-------------------------------')
    total_keys = sum(counts for counts in level_counts.values())
    print(f'Total number of keys across all levels: {total_keys}')
    print('[ Print complete ]')
    print('===============================')
    print()

def main():
    while True:
        try:
            num = int(input("1.insertion 2.deletion. 3.statistic 4.end ▶  "))

            # 1. Insertion
            if num == 1: 
                t = 3 # minimum degree
                B = BTree(t) # make an empty b-tree with the minimum degree t

                insert_file = get_file()
                B = insertion_test(B, insert_file)

            # 2. Deletion
            elif num == 2:
                delete_file = get_file()
                B = deletion_test(B, delete_file)

            # 3. Statistic
            elif num == 3:
                print_statistic(B)

            # 4. End program
            elif num == 4:
                sys.exit(1)

            else:
                print("Invalid input. Please enter 1, 2, 3, or 4.")

        except ValueError:
            print("Invalid input. Please enter a number.")

if __name__ == '__main__':
    main()

