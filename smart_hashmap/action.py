class Action:
    """PipelineManager action representation."""

    def __init__(self, f, priority):
        self.f = f
        self.priority = priority

    def __call__(self, *args, **kwargs):
        return self.f(*args, *kwargs)
