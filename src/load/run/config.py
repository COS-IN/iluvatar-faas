from typing import Dict


class ConfigItem:
    def __init__(self, name, category, default, tree=None):
        self.name = name
        self.category = category
        self.default = default
        self.tree = tree

    def formattable(self) -> bool:
        return self.tree != None

    def update_json(self, json_config):
        c = json_config
        if self.tree == None:
            print(self.name)
        for p in self.tree[:-1]:
            if p in c:
                pass
            else:
                c[p] = dict()
            c = c[p]
        c[self.tree[-1]] = self.default

    def to_env_var(self, prepend: str) -> str:
        ret = prepend
        for t in self.tree:
            ret += "__" + t
        return ret


class LoadConfig:
    def __init__(self):
        self.storage = {}
        self.env_vars = {}

    def __getitem__(self, key):
        return self.storage[key].default

    def __setitem__(self, key, value):
        if key not in self:
            new_item = ConfigItem(key, "UNKNOWN", value, None)
            self.storage[key] = new_item
        self.storage[key].default = value

    def __contains__(self, key):
        return key in self.storage

    def insert(self, item: ConfigItem):
        self.storage[item.name] = item

    def bulk_add(self, category, items, env_var = None):
        if env_var is not None:
            self.env_vars[category] = env_var
        for item in items:
            if len(item) == 2:
                name, default = item
                config_item = ConfigItem(name, category, default, None)
            if len(item) == 3:
                name, default, tree = item
                config_item = ConfigItem(name, category, default, tree)
            self.insert(config_item)

    def overwrite(self, **kwargs):
        for k, v in kwargs.items():
            self[k] = v
        return self

    def to_json(self, category, json_data):
        configs = filter(lambda x: x.category == category, self.storage.values())
        configs = filter(lambda x: x.default != None, configs)
        for item in configs:
            item.update_json(json_data)

    def to_env_var_dict(self, category) -> Dict[str, str]:
        ret = {}
        configs = filter(lambda x: x.category == category, self.storage.values())
        configs = filter(lambda x: x.default is not None, configs)
        configs = filter(lambda x: x.tree is not None, configs)
        for item in configs:
            varname = item.to_env_var(self.env_vars[category])
            ret[varname] = str(item.default)
        return ret
