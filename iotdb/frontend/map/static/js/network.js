/**
 * Provides convenient methods for accessing remote objects
 */
var network = {
    /**
     * Creates an xhr with cors.
     * @param {string} method http method
     * @param {string} url url
     * @return {XMLHttpRequest} with cors enabled
     */
    createCors: function (method, url) {
        var xhr = new XMLHttpRequest();
        xhr.open(method, url, true);
        xhr.withCredentials = true;
        return xhr;
    },
    /**
     * Loads the requested resource with a cors request and calls the callback on success.
     * @param {string} url
     * @param {function} callback
     */
    load: function (url, callback) {
        $.ajax({
            url: url,
            crossDomain: true,
            xhrFields: {
                withCredentials: true
            },
            success: callback
        });
    }
    //todo provide HAL (de)wrapping
};