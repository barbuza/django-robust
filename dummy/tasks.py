from robust import task


def spam():
    raise RuntimeError('fail')


def bar():
    spam()


@task()
def foo():
    bar()
