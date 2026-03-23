import warnings


warnings.filterwarnings(
    "ignore",
    message=r"urllib3 .*doesn't match a supported version!",
    category=Warning,
)
