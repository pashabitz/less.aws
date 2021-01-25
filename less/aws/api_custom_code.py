class BaseCustomCode:
    def before_get(table_config, orm, params, access_token):
        return True

    def after_get(table_config, orm, result_set):
        return True

    def before_insert(table_config, orm, values, access_token):
        return True

    def after_insert(table_config, orm, values):
        return True

    def before_delete(table_config, orm, params, access_token):
        return True

    def after_delete(table_config, orm, params):
        return True

    def before_update(table_config, orm, key, update_values, access_token):
        return True

    def after_update(table_config, orm, key, update_values):
        return True

    def custom_get(path, table_configs, orm, params, access_token=None):
        return False

    def custom_post(path, table_configs, orm, params, values, access_token=None):
        return False

    def custom_delete(path, table_configs, orm, params, access_token=None):
        return False


custom_code_file = None
try:
    custom_code_file = __import__("user_code")
except ModuleNotFoundError:
    pass


class CustomCodeReplaceableWithFile(BaseCustomCode):
    def call_override_if_exists(method_name, pass_through_params):
        NEW_PARAMS = ["access_token"]
        NEW_PARAMS_BY_METHOD = {
            "custom_get": NEW_PARAMS,
            "custom_post": NEW_PARAMS,
            "custom_delete": NEW_PARAMS,
        }
        if custom_code_file and hasattr(custom_code_file, method_name):
            try:
                method_override = getattr(custom_code_file, method_name)
                return method_override(**pass_through_params)
            except TypeError:
                if method_name in NEW_PARAMS_BY_METHOD:
                    for new_param in NEW_PARAMS_BY_METHOD[method_name]:
                        pass_through_params.pop(new_param, None)
                    return method_override(**pass_through_params)
        else:
            base_method = getattr(BaseCustomCode, method_name)
            return base_method(**pass_through_params)

    def before_get(table_config, orm, params, access_token):
        return CustomCodeReplaceableWithFile.call_override_if_exists("before_get", locals())

    def after_get(table_config, orm, result_set):
        return CustomCodeReplaceableWithFile.call_override_if_exists("after_get", locals())

    def before_insert(table_config, orm, values, access_token):
        return CustomCodeReplaceableWithFile.call_override_if_exists("before_insert", locals())

    def after_insert(table_config, orm, values):
        return CustomCodeReplaceableWithFile.call_override_if_exists("after_insert", locals())

    def before_delete(table_config, orm, params, access_token):
        return CustomCodeReplaceableWithFile.call_override_if_exists("before_delete", locals())

    def after_delete(table_config, orm, params):
        return CustomCodeReplaceableWithFile.call_override_if_exists("after_delete", locals())

    def before_update(table_config, orm, key, update_values, access_token):
        return CustomCodeReplaceableWithFile.call_override_if_exists("before_update", locals())

    def after_update(table_config, orm, key, update_values):
        return CustomCodeReplaceableWithFile.call_override_if_exists("after_update", locals())

    def custom_get(path, table_configs, orm, params, access_token=None):
        return CustomCodeReplaceableWithFile.call_override_if_exists("custom_get", locals())

    def custom_post(path, table_configs, orm, params, values, access_token=None):
        return CustomCodeReplaceableWithFile.call_override_if_exists("custom_post", locals())

    def custom_delete(path, table_configs, orm, params, access_token=None):
        return CustomCodeReplaceableWithFile.call_override_if_exists("custom_delete", locals())
