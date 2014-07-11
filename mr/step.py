class Step(object):
    def __init__(self, workflow, name, description, handler, 
                 argument_names=[]):

# TODO(dustin): Finish.

        self.__workflow = workflow
        self.__name = name
        self.__description = description
        self.__handler
        self.__argument_names

    @property
    def workflow(self):
        return self.__workflow

    @property
    def name(self):
        return self.__name

    @property
    def description(self):
        return self.__description

    @property
    def handler(self):
        return self.__handler

    @property
    def argument_names(self):
        return self.__argument_names
