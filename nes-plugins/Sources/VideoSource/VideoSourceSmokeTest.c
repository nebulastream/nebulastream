/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
*/

#include <arv.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

static volatile sig_atomic_t stop_requested;

static void request_stop(int signal_number)
{
    (void) signal_number;
    stop_requested = 1;
}

static int fail_if_error(const char* operation, GError** error)
{
    if (*error == NULL)
    {
        return 0;
    }

    fprintf(stderr, "%s: %s\n", operation, (*error)->message);
    g_clear_error(error);
    return 1;
}

static void dump_enumeration(ArvDevice* device, const char* feature)
{
    GError* error = NULL;
    const char* value;
    const char** values;
    guint count = 0;

    if (arv_device_get_feature(device, feature) == NULL)
    {
        return;
    }

    value = arv_device_get_string_feature_value(device, feature, &error);
    if (error != NULL)
    {
        g_clear_error(&error);
        return;
    }

    printf("  %s = %s", feature, value);
    values = arv_device_dup_available_enumeration_feature_values_as_strings(device, feature, &count, &error);
    if (error == NULL && values != NULL)
    {
        printf(" [");
        for (guint index = 0; index < count; ++index)
        {
            printf("%s%s", index == 0 ? "" : ", ", values[index]);
        }
        printf("]");
        g_free(values);
    }
    else
    {
        g_clear_error(&error);
    }
    printf("\n");
}

static void dump_integer(ArvDevice* device, const char* feature)
{
    GError* error = NULL;
    gint64 value;

    if (arv_device_get_feature(device, feature) == NULL)
    {
        return;
    }

    value = arv_device_get_integer_feature_value(device, feature, &error);
    if (error == NULL)
    {
        printf("  %s = %" G_GINT64_FORMAT "\n", feature, value);
    }
    else
    {
        g_clear_error(&error);
    }
}

static void dump_string(ArvDevice* device, const char* feature)
{
    GError* error = NULL;
    const char* value;

    if (arv_device_get_feature(device, feature) == NULL)
    {
        return;
    }

    value = arv_device_get_string_feature_value(device, feature, &error);
    if (error == NULL)
    {
        printf("  %s = %s\n", feature, value);
    }
    else
    {
        g_clear_error(&error);
    }
}

static void dump_boolean(ArvDevice* device, const char* feature)
{
    GError* error = NULL;
    gboolean value;

    if (arv_device_get_feature(device, feature) == NULL)
    {
        return;
    }

    value = arv_device_get_boolean_feature_value(device, feature, &error);
    if (error == NULL)
    {
        printf("  %s = %s\n", feature, value ? "true" : "false");
    }
    else
    {
        g_clear_error(&error);
    }
}

static void dump_stream_configuration(ArvDevice* device, const char* phase)
{
    static const char* const strings[] = {"DeviceVendorName", "DeviceModelName", "DeviceID"};
    static const char* const enumerations[] = {
        "FLK_TI_StreamDataSourceSelector", "PixelFormat", "TransmitAs", "ImageCompressionMode", "AcquisitionMode"};
    static const char* const integers[] = {
        "DeviceStreamChannelCount",
        "FLK_TI_Info_VLDataSize",
        "FLK_TI_Info_REDataSize",
        "FLK_TI_Info_REDataRate",
        "SensorWidth",
        "SensorHeight",
        "Width",
        "Height",
        "PayloadSize",
        "GevSCPSPacketSize",
        "GevSCPD"};
    static const char* const booleans[] = {
        "FLK_TI_Info_VLDataProviderAvailable", "FLK_TI_Info_REDataProviderAvailable", "FLK_TI_Info_SecuredDataStreamingOnly"};

    printf("Camera configuration %s:\n", phase);
    for (size_t index = 0; index < G_N_ELEMENTS(strings); ++index)
    {
        dump_string(device, strings[index]);
    }
    for (size_t index = 0; index < G_N_ELEMENTS(enumerations); ++index)
    {
        dump_enumeration(device, enumerations[index]);
    }
    for (size_t index = 0; index < G_N_ELEMENTS(integers); ++index)
    {
        dump_integer(device, integers[index]);
    }
    for (size_t index = 0; index < G_N_ELEMENTS(booleans); ++index)
    {
        dump_boolean(device, booleans[index]);
    }
}

int main(void)
{
    GError* error = NULL;
    ArvDevice* device;
    ArvStream* stream;
    gint64 payload_size;
    guint64 processed;
    guint64 failures;
    guint64 underruns;

    setvbuf(stdout, NULL, _IOLBF, 0);
    device = arv_open_device(NULL, &error);
    if (fail_if_error("opening camera", &error) || !ARV_IS_DEVICE(device))
    {
        fprintf(stderr, "could not open camera\n");
        return EXIT_FAILURE;
    }

    gint64 value = arv_device_get_integer_feature_value(device, "FLK_TI_StreamDataSourceSelector", &error);
    printf("FLK_TI_StreamDataSourceSelector = %" G_GINT64_FORMAT "\n", value);
    if (fail_if_error("receiving source selector stream", &error)){
        g_object_unref(device);
        return EXIT_FAILURE;
    }

    arv_device_set_integer_feature_value(device, "FLK_TI_StreamDataSourceSelector", 2, &error);
    if (fail_if_error("selecting interleaved stream", &error))
    {
        g_object_unref(device);
        return EXIT_FAILURE;
    }

    arv_device_set_integer_feature_value(device, "GevSCPSPacketSize", 1444, &error);
    if (fail_if_error("setting GigE packet size", &error))
    {
        g_object_unref(device);
        return EXIT_FAILURE;
    }

    stream = arv_device_create_stream(device, NULL, NULL, &error);
    if (fail_if_error("creating camera stream", &error) || !ARV_IS_STREAM(stream))
    {
        fprintf(stderr, "could not create camera stream\n");
        g_object_unref(device);
        return EXIT_FAILURE;
    }

    dump_stream_configuration(device, "before acquisition");
    payload_size = arv_device_get_integer_feature_value(device, "PayloadSize", &error);
    if (fail_if_error("reading camera payload size", &error))
    {
        g_object_unref(stream);
        g_object_unref(device);
        return EXIT_FAILURE;
    }

    for (unsigned int index = 0; index < 30; ++index)
    {
        arv_stream_push_buffer(stream, arv_buffer_new(payload_size, NULL));
    }

    arv_device_execute_command(device, "AcquisitionStart", &error);
    if (fail_if_error("starting acquisition", &error))
    {
        g_object_unref(stream);
        g_object_unref(device);
        return EXIT_FAILURE;
    }

    printf("Receiving frames (payload %" G_GINT64_FORMAT " bytes); press Ctrl-C to stop.\n", payload_size);
    signal(SIGINT, request_stop);

    while (!stop_requested)
    {
        ArvBuffer* buffer;
        g_usleep(1000000);
        while ((buffer = arv_stream_try_pop_buffer(stream)) != NULL)
        {
            if (arv_buffer_get_status(buffer) == ARV_BUFFER_STATUS_SUCCESS)
            {
                size_t image_size;
                arv_buffer_get_data(buffer, &image_size);
                printf(
                    "frame: %ux%u, %zu bytes, pixel format 0x%08x, timestamp %" G_GUINT64_FORMAT "\n",
                    arv_buffer_get_image_width(buffer),
                    arv_buffer_get_image_height(buffer),
                    image_size,
                    arv_buffer_get_image_pixel_format(buffer),
                    arv_buffer_get_timestamp(buffer));
            }
            else
            {
                fprintf(stderr, "discarded Aravis buffer with status %d\n", arv_buffer_get_status(buffer));
            }

            arv_stream_push_buffer(stream, buffer);
        }
    }

    arv_device_execute_command(device, "AcquisitionStop", NULL);
    arv_stream_get_statistics(stream, &processed, &failures, &underruns);
    printf("Processed buffers = %" G_GUINT64_FORMAT ", failures = %" G_GUINT64_FORMAT ", underruns = %" G_GUINT64_FORMAT "\n",
           processed,
           failures,
           underruns);
    g_object_unref(stream);
    g_object_unref(device);
    return EXIT_SUCCESS;
}
