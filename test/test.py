def gen():
    yield 1
    yield 2
    yield 3

g = gen()         # g 是生成器
print(next(g))    # 输出 1，gen 暂停
print(123)
for v in g:      # 直接迭代生成器，依次得到 2,3
    print(v)