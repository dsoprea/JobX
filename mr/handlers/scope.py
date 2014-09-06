class MrConfigure(object):
    pass


class MrConfigureToMap(MrConfigure):
    def __init__(self, next_step_name):
        self.__next_step_name = next_step_name

    @property
    def next_step_name(self):
        return self.__next_step_name


class MrConfigureToReturn(MrConfigure):
    pass

SCOPE_INJECTED_TYPES = {
    'MrConfigureToMap': MrConfigureToMap,
    'MrConfigureToReturn': MrConfigureToReturn,
}
