"""Click command line helpers."""

from click import Option, UsageError


# From https://gist.github.com/listx/e06c7561bddfe47346e41a23a3026f33 and https://github.com/pallets/click/issues/257
class MutuallyExclusiveOption(Option):
    mutex_groups = {}

    def __init__(self, *args, **kwargs):
        opts_list = kwargs.pop('mutex_group', "")
        self.mutex_group_key = ','.join(opts_list)
        self.mutex_groups[self.mutex_group_key] = 0
        help = kwargs.get('help', '')
        kwargs['help'] = help + (
            ' NOTE: This argument may be one of: '
            '[' + self.mutex_group_key + '].'
        )
        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        if self.name in self.mutex_group_key and self.name in opts:
            self.mutex_groups[self.mutex_group_key] += 1
        if self.mutex_groups[self.mutex_group_key] > 1:
            exclusive_against = self.mutex_group_key.split(',')
            exclusive_against.remove(self.name)
            raise UsageError(
                "Illegal usage: `{}` is mutually exclusive against "
                "arguments {}.".format(
                    self.name,
                    exclusive_against
                )
            )

        return super(MutuallyExclusiveOption, self).handle_parse_result(
            ctx,
            opts,
            args
        )
