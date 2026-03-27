
def patch_openpyxl():
    
    # Monkey patch openpyxl to avoid an error when opening the urban units INSEE file
    # Source : https://stackoverflow.com/questions/71733414/copying-from-a-range-of-cells-with-openpyxl-error-colors-must-be-argb-hex-valu
    from openpyxl.styles.colors import WHITE, RGB
    __old_rgb_set__ = RGB.__set__
    def __rgb_set_fixed__(self, instance, value):
        try:
            __old_rgb_set__(self, instance, value)
        except ValueError as e:
            if e.args[0] == 'Colors must be aRGB hex values':
                __old_rgb_set__(self, instance, WHITE)
    RGB.__set__ = __rgb_set_fixed__