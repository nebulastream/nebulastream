
const analyticsVideo = {
    startVideo: function (element) {
        let videoQuery = $('video#video-background');
        let video = videoQuery[0];
        video.src = analyticsVideo.getVideoSource(element);
        video.play();
        $('#text-analytics-video-modal #content-layer').hide();
        $('#text-analytics-video-modal #frame-background').hide();
        videoQuery.show();
    },

    replaceVideo: function () {
        $("#text-analytics-video-modal #content-layer").show();
        $("#text-analytics-video-modal #frame-background").show();
        $("#text-analytics-video-modal video#video-background").hide();
    },

    finishVideo: function () {
        $("#text-analytics-video-modal video#video-background").get(0).currentTime = 999999;
    },

    getVideoSource: function(element) {
        let index = $('#text-analytics-video-modal .dropdown-menu a').index(element);
        console.debug('index: ', index, 'text: ', element.text);
        return "/assets/videos/sd4m_da-example-" + (index+1) + ".mp4";
    },

    init: function() {
        $(".btn-group ul.dropdown-menu li a").click(function(){
            analyticsVideo.startVideo(this);
        });

        let videoQuery = $('video#video-background');
        videoQuery.bind("ended", function() {
            analyticsVideo.replaceVideo();
        });

        videoQuery.on("click", function() {
            let video = videoQuery[0];
            if (video.paused)
                video.play();
            else
                video.pause();
        });
    }
};
