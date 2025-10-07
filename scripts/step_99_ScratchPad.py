from utils import make_progress_bar

# Iterable test
for i in make_progress_bar(range(5), desc="Test iterable"):
    pass

# Numeric total test
bar = make_progress_bar(total=5, desc="Test numeric")
for i in range(5):
    bar.update(1)
bar.close()
