/**
 * Handles the sliding navigation sidebar
 *
 * @type {{searchArea: (*), slideLeft: (*), close: 
 sideBar.close, slideLeftBtn: Element, oldVal: 
 {}, initSlider: sideBar.initSlider, destroySlider: 
 sideBar.destroySlider, refreshHeatmapSource: 
 sideBar.refreshHeatmapSource, isVisible: 
 sideBar.isVisible, updateAllFilterButtonStates: 
 sideBar.updateAllFilterButtonStates, updateFilterButtonState: 
 sideBar.updateFilterButtonState, toggleFilterSetting: 
 sideBar.toggleFilterSetting, resetFilterSettings: 
 sideBar.resetFilterSettings, init: sideBar.init}}
 */
const sideBar = {
    searchArea: $('#search-area'),

    slideLeft: new Menu({
        wrapper: '#o-wrapper',
        type: 'slide-left',
        menuOpenerClass: '.c-button',
        maskId: '#c-mask'
    }),

    close: function () {
        this.slideLeft.close();
        $('#search-area').val('');
        $('#recommendation-placeholder').empty();
        $(this.slideLeftBtn).show();
    },

    slideLeftBtn: document.querySelector('#c-button--slide-left'),

    oldVal: {},

    initSlider: function () {
        $('.heatmapRangeSlider').removeClass("hidden");
        $('#heatmapRange').ionRangeSlider({
            type: "double",
            hide_min_max: true,
            grid: true,
            grid_snap: true,
            grid_num: 6,
            step: 24 * 60 * 60,
            // max_interval: 30 * 24 * 60 * 60,
            drag_interval: true,
            min: +moment().endOf("day").subtract(32, "days").format("X"),
            max: +moment().endOf("day").format("X"),

            prettify: function (num) {
                return moment(num, "X").format("DD.MM.");
            }
        });
        $("#heatmapRange").data("ionRangeSlider").update({
            from: +moment().endOf("day").subtract(2, "days").format("X"),
            to: +moment().endOf("day").format("X")
        })
    },

    destroySlider: function () {
        if (!$('.heatmapRangeSlider').hasClass("hidden")) {
            $('.heatmapRangeSlider').addClass("hidden");
        }
    },

    disableCompanySearch: function () {
        $("#search-placeholder .input-group").children().prop("disabled", true);
    },

    enableCompanySearch: function () {
        $("#search-placeholder .input-group").children().prop("disabled", false);
    },

    refreshHeatmapSource: function () {
        let slider = $('#heatmapRange').ionRangeSlider();
        var from = moment.unix(slider.data("from")).set({
            'hour': 1,
            'minute': 0,
            'second': 0,
            'milisecond': 0
        }).toISOString();
        var to = moment.unix(slider.data("to")).set({
            'hour': 24,
            'minute': 59,
            'second': 59,
            'milisecond': 59
        }).toISOString();
        var url = config.urls.event.heatmap("0", "0", "15", "56", from, to, 4000);
        network.load(url, function (geojson) {
            socialMap.map.getSource('events').setData(geojson);
        });

    },

    isVisible: function () {
        return !!$('#c-menu--slide-left.is-active').length;
    },

    updateAllFilterButtonStates: function () {
        $('.filter-list li').each(function (index, element) {
            sideBar.updateFilterButtonState(element);
        });
    },

    updateFilterButtonState: function (parentElement) {
        const filterSetting = $(parentElement).data("filter-setting");
        const filterButton = $(parentElement).find('.selection-circle');
        const isSelected = eventFilter.getFilterSetting(filterSetting) === false;

        if (isSelected) {
            filterButton.addClass('selection-selected');
        } else {
            filterButton.removeClass('selection-selected');
        }
    },

    toggleFilterSetting: function (filterElement) {
        const filterSetting = $(filterElement).data("filter-setting");
        eventFilter.toggleFilterSetting(filterSetting);
        this.updateAllFilterButtonStates();
    },

    resetFilterSettings: function () {
        eventFilter.resetFilterSettings();
        this.updateAllFilterButtonStates();
    },

    showLoadingSpinner: function() {
        if ($('#loadingspinner').hasClass('hidden')) {
            $('#loadingspinner').removeClass('hidden');
            $('.heatmapRangeSlider').addClass('with-spinner');
        }
    },

    hideLoadingSpinner: function() {
        if (!$('#loadingspinner').hasClass('hidden')){
            $('#loadingspinner').addClass('hidden');
            $('.heatmapRangeSlider').removeClass('with-spinner');
        }
    },

    init: function () {
        let oldVal = {};
        const searchArea = sideBar.searchArea;
        searchArea.keyup(function () {
            const value = $(this).val();
            if (value !== oldVal) {
                $('#recommendation-placeholder').empty();
                if (value.length >= 2) {
                    const close = $(".leaflet-popup-close-button")[0];
                    if (close != undefined) {
                        close.click();
                    }
                    $('#recommendation-placeholder').empty();
                    network.load(config.urls.company.findByName(value, 5), function (response) {
                        if (!response || response.count == 0)
                            return;

                        const placeholder = $('#recommendation-placeholder');
                        placeholder.empty();

                        socialMap.clearCompanyLayer();
                        $.each(response.results, function (index, doc) {
                            doc = companyMappings.processCompanyResult(doc);

                            if (doc.displayLocation == null || doc.companyName == null)
                                return; // Skip companies without location

                            doc.feedSource = "Company";

                            const marker = MapMarkerBuilder.buildCompanyMarker(doc);
                            socialMap.companyLayer.addLayer(marker);

                            const template = $("<div>" + doc.companyName + "</div>");
                            template.on('mousedown', function (e) {
                                e.stopPropagation();
                                marker.openPopup();
                            });
                            placeholder.append(template);
                        });

                    });
                }
                oldVal = value;
            }
        });
        searchArea.focus(function () {
            $('.invisible-when-searching').hide();
            $('#recommendation-placeholder').show();
        });
        searchArea.blur(function () {
            $('.invisible-when-searching').show();
            $('#recommendation-placeholder').hide();
        });
        $("#recommendation-placeholder").on('mouseover', 'div', function (e) {
            $('#search-area').val($(this).text());
        });

        $('.filter-list li').click(function () {
            sideBar.toggleFilterSetting(this);
        });

        $('#reset-filter').click(function () {
            sideBar.resetFilterSettings();
        });
/*
        $('#icon-filter').popover({
            content: $('#popover-body').clone().toggle("hidden").html(),
            container: 'body',
            placement: 'top',
            html: true
        });*/
        sideBar.slideLeftBtn.addEventListener('click', function (e) {
            e.preventDefault();
            sideBar.slideLeft.open();
            sideBar.updateAllFilterButtonStates();
            $(sideBar.slideLeftBtn).hide();
        });
        $(".c-menu__close").on("click", function () {
            $(sideBar.slideLeftBtn).show()
        });

        $('.map-mode-btn-group input#live:radio[name=map-mode]').on("change", function () {
            if ($(this).is(":checked"))
                feedController.removeHeatmap();
        });
        $('.map-mode-btn-group input#historic:radio[name=map-mode]').on("change", function () {
            if ($(this).is(":checked"))
                feedController.createHeatmap();
        });

        $('#refreshHeatmap').click(function () {
            sideBar.refreshHeatmapSource();
            sideBar.showLoadingSpinner();
        })
    }
};
