window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(feature, context) {

            const {
                classes,
                colorscale,
                style,
                colorProp
            } = context.hideout || {};
            const value = feature.properties[colorProp];

            style.fillColor = 'gray';

            if (value !== undefined && value !== null) {
                for (let i = 0; i < classes.length - 1; ++i) {
                    if (value > classes[i] && value <= classes[i + 1]) {
                        style.fillColor = colorscale[i];
                        break;
                    }
                }
            }

            return style;

        }

    }
});