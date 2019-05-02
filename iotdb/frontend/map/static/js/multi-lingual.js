const multiLingual = {
    lang: new Lang(),

    init: function () {
        multiLingual.lang.dynamic('de', 'data/translation_de.json');
        multiLingual.lang.init({
            defaultLang: 'en',
            currentLang: 'en'
        });
        multiLingual.lang.currentLang = "en";

        $('.lang-btn-group input#lang-en:radio[name=lang-mode]').on("change", function() {
            if ($(this).is(":checked"))
                multiLingual.lang.change("en");
        });
        $('.lang-btn-group input#lang-de:radio[name=lang-mode]').on("change", function() {
            if ($(this).is(":checked"))
                multiLingual.lang.change("de");
        });
    }
};
