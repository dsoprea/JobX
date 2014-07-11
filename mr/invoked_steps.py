class InvokedSteps(object):
    """This collects steps that have been mapped-to by another."""

    def add(self, invoked_step):
# TODO(dustin): Persist this association.
        raise NotImplementedError()

    def reduce(self):
# TODO(dustin): Recall the results from persistence, as a generator, and pass to a reduction step.
# TODO(dustin): Try to use the 'reduce' keyword.
        raise NotImplementedError()
