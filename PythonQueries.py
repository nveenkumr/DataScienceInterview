# Databricks notebook source
# --------------------------------------------
# Python String Manipulation Interview Questions
# Consolidated, de-duplicated list of problems with code and sample outputs
# --------------------------------------------

# 1. Remove Characters from a String
# Problem: Remove all occurrences of a specified character from a string.
# Code:
s = "hello"
result = s.replace("o", "")
print(result)
# Output: hell

# COMMAND ----------

# 2. Count Occurrence of Characters in a String
# Problem: Count how many times each character appears in a string.
# Code:
string = "Python"
dict1 = {}
for ch in string:
    dict1[ch] = string.count(ch)
print(dict1)
# Output: {'P': 1, 'y': 1, 't': 1, 'h': 1, 'o': 1, 'n': 1}

# COMMAND ----------

# 3. Reverse a String
# Problem: Reverse the characters in a string.
# Code:
s1 = "hello"
s2 = []
for ch in s1:
    s2.insert(0, ch)
print("".join(s2))
# Output: olleh

# COMMAND ----------

# 4. Validate Parentheses (Single and Multiple Types)
# Problem: Determine if a string of parentheses is valid (matching and ordered).
# Code:
def is_valid_parentheses(s: str) -> bool:
    stack = []
    pairs = {')': '(', ']': '[', '}': '{'}
    for ch in s:
        if ch in pairs.values():
            stack.append(ch)
        elif ch in pairs:
            if not stack or stack[-1] != pairs[ch]:
                return False
            stack.pop()
    return not stack

print(is_valid_parentheses("({[]})"))  # True
print(is_valid_parentheses("({[)]}"))  # False
# Output: True, False

# COMMAND ----------

# 5. Remove Outermost Parentheses
# Problem: Remove the outermost layer of parentheses from each primitive segment.
# Code:
def remove_outer_parentheses(s: str) -> str:
    stack = 0
    result = []
    for ch in s:
        if ch == '(' and stack > 0:
            result.append(ch)
        if ch == ')':
            stack -= 1
        if ch == ')' and stack > 0:
            result.append(ch)
        if ch == '(':
            stack += 1
    return "".join(result)

print(remove_outer_parentheses("(()())(())"))  # ()()()
# Output: ()()()

# COMMAND ----------

# 6. Check Anagram
# Problem: Check whether two strings are anagrams.
# Code:
def is_anagram(s1: str, s2: str) -> bool:
    return sorted(s1) == sorted(s2)

print(is_anagram("listen", "silent"))  # True
# Output: True

# COMMAND ----------

# 7. Group Anagrams
# Problem: Group a list of strings into anagram clusters.
# Code:
from collections import defaultdict
def group_anagrams(words: list[str]) -> list[list[str]]:
    d = defaultdict(list)
    for w in words:
        key = tuple(sorted(w))
        d[key].append(w)
    return list(d.values())

print(group_anagrams(["eat","tea","tan","ate","nat","bat"]))
# Output: [['eat','tea','ate'], ['tan','nat'], ['bat']]

# COMMAND ----------

# 8. Rotate String Check
# Problem: Check if one string is a rotation of another.
# Code:
def is_rotation(A: str, B: str) -> bool:
    return len(A) == len(B) and B in (A + A)

print(is_rotation("abcde", "cdeab"))  # True
# Output: True

# COMMAND ----------

# 9. Check Palindrome
# Problem: Verify whether a string reads the same forward and backward.
# Code:
def is_palindrome(s: str) -> bool:
    return s == s[::-1]

print(is_palindrome("madam"))  # True
# Output: True

# COMMAND ----------

# 10. Find Missing Number in 1…n
# Problem: In a list of unique numbers from 1 to n with one missing, find the missing number.
# Code:
def find_missing(nums: list[int]) -> int:
    n = len(nums) + 1
    return n * (n + 1) // 2 - sum(nums)

print(find_missing([1,5,6,3,4]))  # 2
# Output: 2

# COMMAND ----------

# 11. Longest Substring Without Repeating Characters
# Problem: Find the length of the longest substring with no repeated characters.
# Code:
def length_of_longest_substr(s: str) -> int:
    used = {}
    start = max_len = 0
    for i, ch in enumerate(s):
        if ch in used and used[ch] >= start:
            start = used[ch] + 1
        used[ch] = i
        max_len = max(max_len, i - start + 1)
    return max_len

print(length_of_longest_substr("abcbdfghjkabc"))  # 10
# Output: 10

# COMMAND ----------

# 12. Longest Palindromic Substring (Expand Around Center)
# Problem: Find the longest palindromic substring.
# Code:
def expand_around_center(s: str, left: int, right: int) -> str:
    while left >= 0 and right < len(s) and s[left] == s[right]:
        left -= 1
        right += 1
    return s[left+1:right]

def longest_palindrome(s: str) -> str:
    result = ""
    for i in range(len(s)):
        p1 = expand_around_center(s, i, i)
        p2 = expand_around_center(s, i, i+1)
        result = max(result, p1, p2, key=len)
    return result

print(longest_palindrome("babad"))  # "bab" or "aba"
# Output: bab (or aba)

# COMMAND ----------

# 13. Next Greatest Palindrome
# Problem: Given an integer, find the next smallest palindrome larger than the number.
# Code:
def find_next_palindrome(number: int) -> int:
    def is_pal(n: str) -> bool:
        return n == n[::-1]
    n = number + 1
    while True:
        if is_pal(str(n)):
            return n
        n += 1

print(find_next_palindrome(123))  # 131
# Output: 131

# COMMAND ----------

# 14. Run-Length Encode a String
# Problem: Compress runs of identical characters into char+count.
# Code:
def run_length_encode(s: str) -> str:
    if not s:
        return ""
    result = []
    count = 1
    for i in range(1, len(s)):
        if s[i] == s[i-1]:
            count += 1
        else:
            result.append(s[i-1] + str(count))
            count = 1
    result.append(s[-1] + str(count))
    return "".join(result)

print(run_length_encode("aaaabbbbccccaaa"))  # a4b4c4a2
# Output: a4b4c4a2

# COMMAND ----------

# End of notebook
