<script type="text/javascript">
    function initThisPage()

    {
        "use strict";

        if ($("#calibre_use_server").is(":checked"))
            {
                    $("#calibre_options").show();
            }
        else
            {
                    $("#calibre_options").hide();
            }
        $("#calibre_use_server").click(function(){
                if ($("#calibre_use_server").is(":checked"))
                {
                        $("#calibre_options").slideDown();
                }
                else
                {
                        $("#calibre_options").slideUp();
                }
        });

        if ($("#gr_sync").is(":checked"))
            {
                    $("#grsync_options").show();
            }
        else
            {
                    $("#grsync_options").hide();
            }

        $("#gr_sync").click(function(){
                if ($("#gr_sync").is(":checked"))
                {
                        $("#grsync_options").slideDown();
                }
                else
                {
                        $("#grsync_options").slideUp();
                }
        });

        if ($("#gr_syncuser").is(":checked"))
            {
                    $("#gruser_options").show();
                    $("#grlibrary_options").hide();
            }
        else
            {
                    $("#grlibrary_options").show();
                    $("#gruser_options").hide();
            }

        $("#gr_syncuser").click(function(){
                if ($("#gr_syncuser").is(":checked"))
                {
                        $("#gruser_options").slideDown();
                        $("#grlibrary_options").slideUp();
                }
                else
                {
                        $("#grlibrary_options").slideDown();
                        $("#gruser_options").slideUp();
                }
        });

        if ($("#user_accounts").is(":checked"))
            {
                    $("#admin_options").show();
                    $("#rss_options").show();
                    $("#webserver_options").hide();
            }
            else
            {
                    $("#webserver_options").show();
                    $("#admin_options").hide();
                    $("#rss_options").hide();
            }

        $("#user_accounts").click(function(){
                if ($("#user_accounts").is(":checked"))
                {
                        $("#webserver_options").slideUp();
                        $("#admin_options").slideDown();
                        $("#rss_options").slideDown();
                }
                else
                {
                        $("#admin_options").slideUp();
                        $("#rss_options").slideUp();
                        $("#webserver_options").slideDown();
                }
        });

        if ($("#https_enabled").is(":checked"))
            {
                    $("#https_options").show();
            }
            else
            {
                    $("#https_options").hide();
            }

        $("#https_enabled").click(function(){
                if ($("#https_enabled").is(":checked"))
                {
                        $("#https_options").slideDown();
                }
                else
                {
                        $("#https_options").slideUp();
                }
        });
        if ($("#ssl_verify").is(":checked"))
            {
                    $("#ssl_options").show();
            }
            else
            {
                    $("#ssl_options").hide();
            }

        $("#ssl_verify").click(function(){
                if ($("#ssl_verify").is(":checked"))
                {
                        $("#ssl_options").slideDown();
                }
                else
                {
                        $("#ssl_options").slideUp();
                }
        });

        if ($("#audio_tab").is(":checked"))
            {
                    $("#graudio_options").show();
            }
            else
            {
                    $("#graudio_options").hide();
            }

        $("#audio_tab").click(function(){
                if ($("#audiobook_enabled").is(":checked"))
                {
                        $("#graudio_options").slideDown();
                }
                else
                {
                        $("#graudio_options").slideUp();
                }
        });

        if ($("#api_enabled").is(":checked"))
            {
                    $("#api_options").show();
            }
            else
            {
                    $("#api_options").hide();
            }

        $("#api_enabled").click(function(){
                if ($("#api_enabled").is(":checked"))
                {
                        $("#api_options").slideDown();
                }
                else
                {
                        $("#api_options").slideUp();
                }
        });

        if ($("#show_direct_prov").is(":checked"))
            {
                    $("#direct_prov").show();
            }
        else
            {
                    $("#direct_prov").hide();
            }

        $("#show_direct_prov").click(function(){
                if ($("#show_direct_prov").is(":checked"))
                {
                        $("#direct_prov").slideDown();
                }
                else
                {
                        $("#direct_prov").slideUp();
                }
        });

        if ($("#show_newz_prov").is(":checked"))
            {
                    $("#newz_prov").show();
            }
        else
            {
                    $("#newz_prov").hide();
            }

        $("#show_newz_prov").click(function(){
                if ($("#show_newz_prov").is(":checked"))
                {
                        $("#newz_prov").slideDown();
                }
                else
                {
                        $("#newz_prov").slideUp();
                }
        });

        if ($("#show_torz_prov").is(":checked"))
            {
                    $("#torz_prov").show();
            }
        else
            {
                    $("#torz_prov").hide();
            }

        $("#show_torz_prov").click(function(){
                if ($("#show_torz_prov").is(":checked"))
                {
                        $("#torz_prov").slideDown();
                }
                else
                {
                        $("#torz_prov").slideUp();
                }
        });

        if ($("#show_rss_prov").is(":checked"))
            {
                    $("#rss_prov").show();
            }
        else
            {
                    $("#rss_prov").hide();
            }

        $("#show_rss_prov").click(function(){
                if ($("#show_rss_prov").is(":checked"))
                {
                        $("#rss_prov").slideDown();
                }
                else
                {
                        $("#rss_prov").slideUp();
                }
        });

         if ($("#show_tor_prov").is(":checked"))
            {
                    $("#tor_prov").show();
            }
        else
            {
                    $("#tor_prov").hide();
            }

        $("#show_tor_prov").click(function(){
                if ($("#show_tor_prov").is(":checked"))
                {
                        $("#tor_prov").slideDown();
                }
                else
                {
                        $("#tor_prov").slideUp();
                }
        });

        if ($("#show_irc_prov").is(":checked"))
            {
                    $("#irc_prov").show();
            }
        else
            {
                    $("#irc_prov").hide();
            }

        $("#show_irc_prov").click(function(){
                if ($("#show_irc_prov").is(":checked"))
                {
                        $("#irc_prov").slideDown();
                }
                else
                {
                        $("#irc_prov").slideUp();
                }
        });

       if ($("#tor_downloader_blackhole").is(":checked"))
            {
                    $("#tor_blackhole_options").show();
            }
        else
            {
                    $("#tor_blackhole_options").hide();
            }

        $("#tor_downloader_blackhole").click(function(){
                if ($("#tor_downloader_blackhole").is(":checked"))
                {
                        $("#tor_blackhole_options").slideDown();
                }
                else
                {
                        $("#tor_blackhole_options").slideUp();
                }
        });

        if ($("#tor_downloader_deluge").is(":checked"))
            {
                    $("#deluge_options").show();
            }
        else
            {
                    $("#deluge_options").hide();
            }
        $("#tor_downloader_deluge").click(function(){
                if ($("#tor_downloader_deluge").is(":checked"))
                {
                        $("#deluge_options").slideDown();
                }
                else
                {
                        $("#deluge_options").slideUp();
                }
        });

        if ($("#tor_downloader_transmission").is(":checked"))
            {
                    $("#transmission_options").show();
            }
        else
            {
                    $("#transmission_options").hide();
            }
        $("#tor_downloader_transmission").click(function(){
                if ($("#tor_downloader_transmission").is(":checked"))
                {
                        $("#transmission_options").slideDown();
                }
                else
                {
                        $("#transmission_options").slideUp();
                }
        });

        if ($("#tor_downloader_utorrent").is(":checked"))
            {
                    $("#utorrent_options").show();
            }
        else
            {
                    $("#utorrent_options").hide();
            }

        $("#tor_downloader_utorrent").click(function(){
                if ($("#tor_downloader_utorrent").is(":checked"))
                {
                        $("#utorrent_options").slideDown();
                }
                else
                {
                        $("#utorrent_options").slideUp();
                }
        });

        if ($("#tor_downloader_rtorrent").is(":checked"))
            {
                    $("#rtorrent_options").show();
            }
        else
            {
                    $("#rtorrent_options").hide();
            }

        $("#tor_downloader_rtorrent").click(function(){
                if ($("#tor_downloader_rtorrent").is(":checked"))
                {
                        $("#rtorrent_options").slideDown();
                }
                else
                {
                        $("#rtorrent_options").slideUp();
                }
        });

        if ($("#tor_downloader_qbittorrent").is(":checked"))
            {
                    $("#qbittorrent_options").show();
            }
        else
            {
                    $("#qbittorrent_options").hide();
            }

        $("#tor_downloader_qbittorrent").click(function(){
                if ($("#tor_downloader_qbittorrent").is(":checked"))
                {
                        $("#qbittorrent_options").slideDown();
                }
                else
                {
                        $("#qbittorrent_options").slideUp();
                }
        });

        if ($("#nzb_downloader_blackhole").is(":checked"))
            {
                    $("#nzb_blackhole_options").show();
            }
        else
            {
                    $("#nzb_blackhole_options").hide();
            }

        $("#nzb_downloader_blackhole").click(function(){
                if ($("#nzb_downloader_blackhole").is(":checked"))
                {
                        $("#nzb_blackhole_options").slideDown();
                }
                else
                {
                        $("#nzb_blackhole_options").slideUp();
                }
        });

        if ($("#nzb_downloader_sabnzbd").is(":checked"))
            {
                    $("#sabnzbd_options").show();
            }
        else
            {
                    $("#sabnzbd_options").hide();
            }
        $("#nzb_downloader_sabnzbd").click(function(){
                if ($("#nzb_downloader_sabnzbd").is(":checked"))
                {
                        $("#sabnzbd_options").slideDown();
                }
                else
                {
                        $("#sabnzbd_options").slideUp();
                }
        });

        if ($("#nzb_downloader_nzbget").is(":checked"))
            {
                    $("#nzbget_options").show();
            }
        else
            {
                    $("#nzbget_options").hide();
            }
        $("#nzb_downloader_nzbget").click(function(){
                if ($("#nzb_downloader_nzbget").is(":checked"))
                {
                        $("#nzbget_options").slideDown();
                }
                else
                {
                        $("#nzbget_options").slideUp();
                }
        });

        if ($("#use_synology").is(":checked"))
            {
                    $("#synology_options").show();
            }
        else
            {
                    $("#synology_options").hide();
            }
        $("#use_synology").click(function(){
                if ($("#use_synology").is(":checked"))
                {
                        $("#synology_options").slideDown();
                }
                else
                {
                        $("#synology_options").slideUp();
                }
        });

        $('#generate_api').click(function () {
            $.get("generate_api",
                function (data) { });
        });

        $('#showblocked').on('click', function(e) {
            $.get('showblocked', function(data) {
                bootbox.dialog({
                    title: 'Provider Status',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        prompt: {
                            label: "Clear Blocklist",
                            className: 'btn-danger',
                            callback: function(result){ $.get("clearblocked", function(e) {}); }
                        },
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        if ($("#rss_enabled").is(":checked"))
          {
            $("#rssoptions").show();
          }
          else
          {
              $("#rssoptions").hide();
          }

        $("#rss_enabled").click(function(){
          if ($("#rss_enabled").is(":checked"))
          {
            $("#rssoptions").slideDown();
          }
          else
          {
            $("#rssoptions").slideUp();
          }
        });

        if ($("#opds_enabled").is(":checked"))
          {
            $("#opdsoptions").show();
          }
          else
          {
              $("#opdsoptions").hide();
          }

        $("#opds_enabled").click(function(){
          if ($("#opds_enabled").is(":checked"))
          {
            $("#opdsoptions").slideDown();
          }
          else
          {
            $("#opdsoptions").slideUp();
          }
        });

        if ($("#opds_authentication").is(":checked"))
          {
            $("#opdscredentials").show();
          }
          else
          {
            $("#opdscredentials").hide();
          }

        $("#opds_authentication").click(function(){
          if ($("#opds_authentication").is(":checked"))
          {
            $("#opdscredentials").slideDown();
          }
          else
          {
            $("#opdscredentials").slideUp();
          }
        });

        $("button[role='testprov']").on('click', function(e) {
            var prov = $(this).val();
            var host = ""
            var api = ""
            if ( 'KAT TPB WWT ZOO TDL TRF LIME'.indexOf(prov) >= 0 ) {
                var host = $("#" + prov.toLowerCase() + "_host").val();
                var api = $("#" + prov.toLowerCase() + "_seeders").val();
            }
            if ( 'BOK BFI'.indexOf(prov) >= 0 ) {
                var host = $("#" + prov.toLowerCase() + "_host").val();
            }
            if ( prov.indexOf('gen_') == 0 ) {
                var host = $("#" + prov.toLowerCase() + "_host").val();
                var api = $("#" + prov.toLowerCase() + "_search").val();
            }
            if ( prov.indexOf('newznab_') == 0 ) {
                var host = $("#" + prov.toLowerCase() + "_host").val();
                var api = $("#" + prov.toLowerCase() + "_api").val();
            }
            if ( prov.indexOf('torznab_') == 0 ) {
                var host = $("#" + prov.toLowerCase() + "_host").val();
                var ap = $("#" + prov.toLowerCase() + "_api").val();
                var seed = $("#" + prov.toLowerCase() + "_seeders").val();
                var api = ap + ' : ' + seed
            }
            if ( prov.indexOf('rss_') == 0 ) {
                var host = $("#" + prov.toLowerCase() + "_host").val();
            }
            if ( prov.indexOf('irc_') == 0 ) {
                var server = $("#" + prov.toLowerCase() + "_server").val();
                var channel = $("#" + prov.toLowerCase() + "_channel").val();
                var host = server + ' : ' + channel
                var nick = $("#" + prov.toLowerCase() + "_botnick").val();
                var pass = $("#" + prov.toLowerCase() + "_botpass").val();
                var search = $("#" + prov.toLowerCase() + "_search").val();
                var api = nick + ' : ' + pass + ' : ' + search
            }
            if ( prov.indexOf('apprise_') == 0 ) {
                var host = $("#" + prov.toLowerCase() + "_url").val();
                var s = ($("#" + prov.toLowerCase() + "_snatch").prop('checked') == true) ? '1' : '0';
                var d = ($("#" + prov.toLowerCase() + "_download").prop('checked') == true) ? '1' : '0';
                var api = s + ':' + d
            }
            $("#myAlert").removeClass('hidden');
            $.get('testprovider', {'name': prov, 'host': host, 'api': api},
            function(data) {
                $("#myAlert").addClass('hidden');
                bootbox.dialog({
                    title: 'Test Result',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#show_stats').on('click', function(e) {
            $.get('show_stats', function(data) {
                bootbox.dialog({
                    title: 'Database Stats',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#show_jobs').on('click', function(e) {
            $.get('show_jobs', function(data) {
                bootbox.dialog({
                    title: 'Job Status',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        stopit: {
                            label: "<i class=\"fa fa-ban\"></i> Stop Jobs",
                            className: 'btn-warning',
                            callback: function(result){ $.get("stop_jobs", function(e) {}); }
                        },
                        restart: {
                            label: "<i class=\"fa fa-sync\"></i> Restart Jobs",
                            className: 'btn-info',
                            callback: function(result){ $.get("restart_jobs", function(e) {}); }
                        },
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#show_apprise').on('click', function(e) {
            $.get('show_apprise', function(data) {
                bootbox.dialog({
                    title: 'Supported Types',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#test_sabnzbd').on('click', function() {
            var host = $.trim($("#sab_host").val());
            var port = $.trim($("#sab_port").val());
            var user = $.trim($("#sab_user").val());
            var pwd = $.trim($("#sab_pass").val());
            var api = $.trim($("#sab_api").val());
            var cat = $.trim($("#sab_cat").val());
            var subdir = $.trim($("#sab_subdir").val());
            $.get("test_sabnzbd", {'host': host, 'port': port, 'user': user, 'pwd': pwd, 'api': api, 'cat': cat, 'subdir': subdir},
            function(data) {
                bootbox.dialog({
                    title: 'SABnzbd Connection',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#test_nzbget').on('click', function(e) {
            var host = $.trim($("#nzbget_host").val());
            var port = $.trim($("#nzbget_port").val());
            var user = $.trim($("#nzbget_user").val());
            var pwd = $.trim($("#nzbget_pass").val());
            var cat = $.trim($("#nzbget_category").val());
            var pri = $.trim($("#nzbget_priority").val());
            $.get('test_nzbget', {'host': host, 'port': port, 'user': user, 'pwd': pwd, 'cat': cat, 'pri': pri},
                function(data) {
                bootbox.dialog({
                    title: 'NZBget Connection',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#test_synology').on('click', function(e) {
            var host = $.trim($("#synology_host").val());
            var port = $.trim($("#synology_port").val());
            var user = $.trim($("#synology_user").val());
            var pwd = $.trim($("#synology_pass").val());
            var dir = $.trim($("#synology_dir").val());
            $.get('test_synology', {'host': host, 'port': port, 'user': user, 'pwd': pwd, 'dir': dir},
                function(data) {
                bootbox.dialog({
                    title: 'Synology Connection',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#test_deluge').on('click', function() {
            var host = $.trim($("#deluge_host").val());
            var base = $.trim($("#deluge_base").val());
            var cert = $.trim($("#deluge_cert").val());
            var port = $.trim($("#deluge_port").val());
            var user = $.trim($("#deluge_user").val());
            var pwd = $.trim($("#deluge_pass").val());
            var label = $.trim($("#deluge_label").val());
            $.get("test_deluge", {'host': host, 'port': port, 'base': base, 'cert': cert, 'user': user, 'pwd': pwd, 'label': label},
                function(data) {
                    bootbox.dialog({
                    title: 'Deluge Connection',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#test_transmission').on('click', function(e) {
            var host = $.trim($("#transmission_host").val());
            var port = $.trim($("#transmission_port").val());
            var base = $.trim($("#transmission_base").val());
            var user = $.trim($("#transmission_user").val());
            var pwd = $.trim($("#transmission_pass").val());
            $.get('test_transmission', {'host': host, 'port': port, 'base': base, 'user': user, 'pwd': pwd},
                function(data) {
                bootbox.dialog({
                    title: 'Transmission Connection',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#test_qbittorrent').on('click', function() {
            var host = $.trim($("#qbittorrent_host").val());
            var port = $.trim($("#qbittorrent_port").val());
            var base = $.trim($("#qbittorrent_base").val());
            var user = $.trim($("#qbittorrent_user").val());
            var pwd = $.trim($("#qbittorrent_pass").val());
            var label = $.trim($("#qbittorrent_label").val());
            $.get('test_qbittorrent', {'host': host, 'port': port, 'base': base, 'user': user, 'pwd': pwd, 'label': label},
                function(data) {
                bootbox.dialog({
                    title: 'qBittorrent Connection',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#test_utorrent').on('click', function(e) {
            var host = $.trim($("#utorrent_host").val());
            var port = $.trim($("#utorrent_port").val());
            var base = $.trim($("#utorrent_base").val());
            var user = $.trim($("#utorrent_user").val());
            var pwd = $.trim($("#utorrent_pass").val());
            var label = $.trim($("#utorrent_label").val());
            $.get('test_utorrent', {'host': host, 'port': port, 'base': base, 'user': user, 'pwd': pwd, 'label': label},
                function(data) {
                bootbox.dialog({
                    title: 'uTorrent Connection',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#test_rtorrent').on('click', function(e) {
            var host = $.trim($("#rtorrent_host").val());
            var dir = $.trim($("#rtorrent_dir").val());
            var user = $.trim($("#rtorrent_user").val());
            var pwd = $.trim($("#rtorrent_pass").val());
            var label = $.trim($("#rtorrent_label").val());
            $.get('test_rtorrent', {'host': host, 'dir': dir, 'user': user, 'pwd': pwd, 'label': label},
                function(data) {
                bootbox.dialog({
                    title: 'rTorrent Connection',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        if ($("#use_twitter").is(":checked"))
                {
                        $("#twitteroptions").show();
                }
        else
                {
                        $("#twitteroptions").hide();
                }

        $("#use_twitter").click(function(){
                if ($("#use_twitter").is(":checked"))
                {
                        $("#twitteroptions").slideDown();
                }
                else
                {
                        $("#twitteroptions").slideUp();
                }
        });

        if ($("#use_boxcar").is(":checked"))
                {
                        $("#boxcaroptions").show();
                }
        else
                {
                        $("#boxcaroptions").hide();
                }

        $("#use_boxcar").click(function(){
                if ($("#use_boxcar").is(":checked"))
                {
                        $("#boxcaroptions").slideDown();
                }
                else
                {
                        $("#boxcaroptions").slideUp();
                }
        });

        if ($("#fullscan").is(":checked"))
                {
                        $("#fullscanoptions").show();
                }
        else
                {
                        $("#fullscanoptions").hide();
                }

        $("#fullscan").click(function(){
                if ($("#fullscan").is(":checked"))
                {
                        $("#fullscanoptions").slideDown();
                }
                else
                {
                        $("#fullscanoptions").slideUp();
                }
        });

        if ($("#use_pushbullet").is(":checked"))
                {
                        $("#pushbulletoptions").show();
                }
        else
                {
                        $("#pushbulletoptions").hide();
                }

        $("#use_pushbullet").click(function(){
                if ($("#use_pushbullet").is(":checked"))
                {
                        $("#pushbulletoptions").slideDown();
                }
                else
                {
                        $("#pushbulletoptions").slideUp();
                }
        });

        if ($("#use_pushover").is(":checked"))
                {
                        $("#pushoveroptions").show();
                }
        else
                {
                        $("#pushoveroptions").hide();
                }
        $("#use_pushover").click(function(){
                if ($("#use_pushover").is(":checked"))
                {
                        $("#pushoveroptions").slideDown();
                }
                else
                {
                        $("#pushoveroptions").slideUp();
                }
        });

        if ($("#use_androidpn").is(":checked"))
                {
                        $("#androidpnoptions").show();
                }
        else
                {
                        $("#androidpnoptions").hide();
                }
        $("#use_androidpn").click(function(){
                if ($("#use_androidpn").is(":checked"))
                {
                    $("#androidpnoptions").slideDown();
                }
                else
                {
                    $("#androidpnoptions").slideUp();
                }
        });

        if ($("#androidpn_broadcast").is(":checked"))
                {
                        $("#androidpn_username").hide();
                }
        else
                {
                        $("#androidpn_username").show();
                }
        $("#androidpn_broadcast").click(function(){
                if ($("#androidpn_broadcast").is(":checked"))
                {
                    $("#androidpn_username").slideUp();
                }
                else
                {
                    $("#androidpn_username").slideDown();
                }
        });

            $("#use_prowl").click(function(){
                    if ($("#use_prowl").is(":checked"))
                    {
                            $("#prowloptions").slideDown();
                    }
                    else
                    {
                            $("#prowloptions").slideUp();
                    }
            });

            if ($("#use_prowl").is(":checked"))
                    {
                            $("#prowloptions").show();
                    }
            else
                    {
                            $("#prowloptions").hide();
                    }

            $("#use_growl").click(function(){
                    if ($("#use_growl").is(":checked"))
                    {
                            $("#growloptions").slideDown();
                    }
                    else
                    {
                            $("#growloptions").slideUp();
                    }
            });

            if ($("#use_growl").is(":checked"))
                    {
                            $("#growloptions").show();
                    }
            else
                    {
                            $("#growloptions").hide();
                    }

            $("#use_telegram").click(function(){
                    if ($("#use_telegram").is(":checked"))
                    {
                            $("#telegramoptions").slideDown();
                    }
                    else
                    {
                            $("#telegramoptions").slideUp();
                    }
            });

            if ($("#use_telegram").is(":checked"))
                    {
                            $("#telegramoptions").show();
                    }
            else
                    {
                            $("#telegramoptions").hide();
                    }

        if ($("#use_slack").is(":checked"))
                {
                        $("#slackoptions").show();
                }
        else
                {
                        $("#slackoptions").hide();
                }

        $("#use_slack").click(function(){
                if ($("#use_slack").is(":checked"))
                {
                        $("#slackoptions").slideDown();
                }
                else
                {
                        $("#slackoptions").slideUp();
                }
        });

        if ($("#use_custom").is(":checked"))
                {
                        $("#customoptions").show();
                }
        else
                {
                        $("#customoptions").hide();
                }

        $("#use_custom").click(function(){
                if ($("#use_custom").is(":checked"))
                {
                        $("#customoptions").slideDown();
                }
                else
                {
                        $("#customoptions").slideUp();
                }
        });

        if ($("#use_email").is(":checked"))
                {
                        $("#emailoptions").show();

                        if ($("#use_email_custom_format").is(":checked")) {
                            $("#email_custom_format_options").show();
                        }
                        else {
                            $("#email_custom_format_options").hide();
                        }
                }
        else
                {
                        $("#emailoptions").hide();
                }

        $("#use_email").click(function(){
                if ($("#use_email").is(":checked"))
                {
                        $("#emailoptions").slideDown();
                }
                else
                {
                        $("#emailoptions").slideUp();
                }
        });

        $("#use_email_custom_format").click(function(){
                if ($("#use_email_custom_format").is(":checked"))
                {
                        $("#email_custom_format_options").slideDown();
                }
                else
                {
                        $("#email_custom_format_options").slideUp();
                }
        });

        $('#sysinfo').on('click', function(e) {
            $.get('log_header', function(data) {
                bootbox.dialog({
                    title: 'System Info',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#savefilters').on('click', function(e) {
            $.get('save_filters', function(data) {
                bootbox.dialog({
                    title: 'Export Filters',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });

        $('#loadfilters').on('click', function(e) {
            $.get('load_filters', function(data) {
                bootbox.dialog({
                    title: 'Import Filters',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary'
                        }
                    }
                });
            });
        });


        $('#test_grauth').click(function () {
            var gr_api = $.trim($("#gr_api").val());
            var gr_secret = $.trim($("#gr_secret").val());
            var oauth_token = $.trim($("#gr_oauth_token").val());
            var oauth_secret = $.trim($("#gr_oauth_secret").val());
            $.get("test_grauth", {'gr_api': gr_api, 'gr_secret': gr_secret, 'oauth_token': oauth_token, 'oauth_secret': oauth_secret},
                function (data) {
                    bootbox.dialog({
                        title: 'GoodReads Auth',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#grauth_step1').click(function () {
            var gr_api = $.trim($("#gr_api").val());
            var gr_secret = $.trim($("#gr_secret").val());
            $.get("grauth_step1", {'gr_api': gr_api, 'gr_secret': gr_secret},
                function (data) {
                if ( data.substr(0, 4) == 'http') { bootbox.dialog({
                        title: 'GoodReads Auth',
                        message: '<pre>A new tab or page should open at GoodReads to authorise lazylibrarian. Follow the prompts, then go back to LazyLibrarian and request oAuth2\nIf the page does not open, visit this link...\n'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });  window.open(data);
                }
                else { bootbox.dialog({
                        title: 'GoodReads Response',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                }
              })
        });

        $('#grauth_step2').click(function () {
            $.get("grauth_step2", {},
                function (data) { bootbox.dialog({
                        title: 'GoodReads Confirm',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary',
                                callback: function(){ document.location.reload(true); }
                            }
                        }
                    });
                })
        });


        $('#twitter_step1').click(function () {
            $('#testTwitter-result').html('');
            $.get("twitter_step1", function (data) {window.open(data); })
                .done(function () { $('#testTwitter-result').html('<b>Step1:</b> Confirm Authorization'); });
        });

        $('#twitter_step2').click(function () {
            $('#testTwitter-result').html('');
            var twitter_key = $("#twitter_key").val();
            $.get("twitter_step2", {'key': twitter_key},
                function (data) { $('#testTwitter-result').html(data); });
        });

        $('#test_twitter').click(function () {
            $.get("test_twitter", {},
                function (data) {
                    bootbox.dialog({
                        title: 'Twitter Notifier',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#test_boxcar').click(function () {
            var token = $.trim($("#boxcar_token").val());
            $.get("test_boxcar", {'token': token},
                function (data) {
                    bootbox.dialog({
                        title: 'Boxcar Notifier',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#test_pushbullet').click(function () {
            var token = $.trim($("#pushbullet_token").val());
            var device = $.trim($("#pushbullet_deviceid").val());
            $.get("test_pushbullet", {'token': token, 'device': device},
                function (data) {
                    bootbox.dialog({
                        title: 'Pushbullet Notifier',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
            });

        $('#test_pushover').click(function () {
            var token = $.trim($("#pushover_apitoken").val());
            var keys = $.trim($("#pushover_keys").val());
            var priority = $.trim($("#pushover_priority").val());
            var device = $.trim($("#pushover_device").val());
            $.get("test_pushover", {'apitoken': token, 'keys': keys, 'priority': priority, 'device': device},
                function (data) {
                    bootbox.dialog({
                        title: 'Pushover Notifier',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#test_prowl').click(function () {
            var apikey = $.trim($("#prowl_apikey").val());
            var priority = $.trim($("#prowl_priority").val());
            $.get("test_prowl", {'apikey': apikey, 'priority': priority},
                function (data) {
                    bootbox.dialog({
                        title: 'Prowl Notifier',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#test_growl').click(function () {
            var host = $.trim($("#growl_host").val());
            var password = $.trim($("#growl_password").val());
            $.get("test_growl", {'host': host, 'password': password},
                function (data) {
                    bootbox.dialog({
                        title: 'Growl Notifier',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#test_telegram').click(function () {
            var token = $.trim($("#telegram_token").val());
            var userid = $.trim($("#telegram_userid").val());
            $.get("test_telegram", {'token': token, 'userid': userid},
                function (data) {
                    bootbox.dialog({
                        title: 'Telegram Notifier',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#test_slack').click(function () {
            var token = $.trim($("#slack_token").val());
            var url = $.trim($("#slack_url").val());
            $.get("test_slack", {'token': token, 'url': url},
                function (data) {
                    bootbox.dialog({
                        title: 'Slack Notifier',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#test_custom').click(function () {
            var script = $.trim($("#custom_script").val());
            $.get("test_custom", {'script': script},
                function (data) {
                    bootbox.dialog({
                        title: 'Custom Notifier',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#test_email').click(function () {
            var tls = ($("#email_tls").prop('checked') == true) ? 'True' : 'False';
            var ssl = ($("#email_ssl").prop('checked') == true) ? 'True' : 'False';
            var sendfile = ($("#email_sendfile_ondownload").prop('checked') == true) ? 'True' : 'False';
            var emailfrom = $.trim($("#email_from").val());
            var emailto = $.trim($("#email_to").val());
            var server = $.trim($("#email_smtp_server").val());
            var user = $.trim($("#email_smtp_user").val());
            var password = $.trim($("#email_smtp_password").val());
            var port = $.trim($("#email_smtp_port").val());
            $.get("test_email", {'tls': tls, 'ssl': ssl, 'emailfrom': emailfrom, 'emailto': emailto, 'server': server, 'user': user, 'password': password, 'port': port, 'sendfile': sendfile},
                function (data) {
                    bootbox.dialog({
                        title: 'Email Notifier',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $("#test_androidpn").click(function () {
            var androidpn_url = $.trim($("#androidpn_url").val());
            var androidpn_username = $.trim($("#androidpn_username").val());
            var androidpn_broadcast = ($("#androidpn_broadcast").prop('checked') == true) ? 'Y' : 'N';
            $.get("test_androidpn", {'url': androidpn_url, 'username': androidpn_username, 'broadcast': androidpn_broadcast},
                function (data) {
                    bootbox.dialog({
                        title: 'Android Notifier',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#test_calibredb').click(function () {
            var prg = $.trim($("#imp_calibredb").val());
            $.get("test_calibredb", { 'prg': prg},
                function (data) {
                    bootbox.dialog({
                        title: 'CalibreDB',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });
        $('#test_ebookconvert').click(function () {
            var prg = $.trim($("#ebook_convert").val());
            $.get("test_ebook_convert", { 'prg': prg},
                function (data) {
                    bootbox.dialog({
                        title: 'ebook-convert',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#test_ffmpeg').click(function () {
            var prg = $.trim($("#ffmpeg").val());
            $.get("test_ffmpeg", { 'prg': prg},
                function (data) {
                    bootbox.dialog({
                        title: 'FFMPEG',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#test_preprocessor').click(function () {
            var prg = $.trim($("#ext_preprocessor").val());
            $.get("test_preprocessor", { 'prg': prg},
                function (data) {
                    bootbox.dialog({
                        title: 'PreProcessor',
                        message: '<pre>'+data+'</pre>',
                        buttons: {
                            primary: {
                                label: "Close",
                                className: 'btn-primary'
                            }
                        }
                    });
                });
        });

        $('#http_look').change(function() {
            if ($(this).val() == 'bookstrap') {
                $('#bookstrap_options').removeClass("hidden");
            } else {
                $('#bookstrap_options').addClass("hidden");
            }
        });

        // when the page first loads, hide all tab headers and panels
        $("li[role='presentation']").attr("aria-selected", "false");
        $("li[role='presentation']").removeClass('active');
        //$("div[role='tabpanel']").attr("aria-hidden", "true");
        $("div[role='tabpanel']").removeClass('active');
        // which one do we want to show
        var tabnum = $("#current_tab").val();
        var tabid = $("#" + tabnum);
        var tabpanel = tabid.attr('aria-controls');
        var tabpanelid = $("#" + tabpanel);
        // show the tab header and panel we want
        //tabpanelid.attr("aria-hidden", "false");
        tabpanelid.addClass('active');
        tabid.attr("aria-selected", "true");
        tabid.addClass('active');
        eraseCookie("configTab");
        createCookie("configTab", tabnum, 0);
        $("div[role='tab-table']").removeClass('hidden');

        // when a tab is clicked
        $("li[role='presentation']").click(function(){
            var tabnum = $(this).attr('id');    // store current tab for python
            eraseCookie("configTab");
            createCookie("configTab", tabnum, 0);
            $("#current_tab").val(tabnum);
        });


       $('#checkforupdates').on('click', function(e) {
            eraseCookie("ignoreUpdate");
            $("#myAlert").removeClass('hidden');
            $.get('check_for_updates', function(data) {
                $("#myAlert").addClass('hidden');
                bootbox.dialog({
                    title: 'Check Version',
                    message: '<pre>'+data+'</pre>',
                    buttons: {
                        primary: {
                            label: "Close",
                            className: 'btn-primary',
                            callback: function(){ location.reload(true); }
                        },
                    }
                });
            });
        });

        $("form #bookstrap_theme").on("change", function() {
            $("head #theme").attr("href", "https://maxcdn.bootstrapcdn.com/bootswatch/3.3.7/" + $(this).val() + "/bootstrap.min.css");
        });
    }
</script>
