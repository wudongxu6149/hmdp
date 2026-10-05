package com.hmdp;

import com.hmdp.controller.UploadController;
import com.hmdp.dto.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import org.springframework.mock.web.MockMultipartFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class UploadControllerTest {

    @TempDir
    Path imageDirectory;

    @Test
    void containerUploadPathIsUsedForUploadAndDelete() {
        new ApplicationContextRunner()
                .withUserConfiguration(UploadController.class)
                .withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                        new SystemEnvironmentPropertySource("dockerEnvironment",
                                Map.of("HMDP_UPLOAD_PATH", imageDirectory.toString()))))
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    UploadController controller = context.getBean(UploadController.class);
                    byte[] contents = new byte[]{1, 2, 3};
                    Result result = controller.uploadImage(
                            new MockMultipartFile("file", "demo.png", "image/png", contents));

                    assertThat(result.getSuccess()).isTrue();
                    String imageName = (String) result.getData();
                    Path savedImage = imageDirectory.resolve(imageName.substring(1));
                    assertThat(Files.readAllBytes(savedImage)).isEqualTo(contents);
                    assertThat(controller.deleteBlogImg(imageName).getSuccess()).isTrue();
                    assertThat(savedImage).doesNotExist();
                });
    }
}
