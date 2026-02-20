import os
from ShortGen.audio.voice_module import VoiceModule
from elevenlabs.client import ElevenLabs
from elevenlabs import VoiceSettings
import io


class ElevenLabsVoiceModule(VoiceModule):
    def __init__(self, api_key, voice_id="JBFqnCBsd6RMkjVDRZzb", model_id="eleven_flash_v2_5"):
        self.api_key = api_key
        self.voice_id = voice_id
        self.model_id = model_id
        self.client = ElevenLabs(api_key=api_key)

        # Apply the voice settings
        self.set_voice_settings(
            speed=1.13,
            stability=0.6,
            similarity_boost=0.75,
            style=0.2
        )

        super().__init__()

    def set_voice_settings(self, speed=1.0, stability=0.5, similarity_boost=0.75, style=0.0):
        """Set voice settings for the ElevenLabs voice."""
        try:
            self.client.voices.edit_settings(
                voice_id=self.voice_id,
                request=VoiceSettings(
                    stability=stability,
                    similarity_boost=similarity_boost,
                    style=style,
                    speed=speed
                )
            )
            print(
                f"Voice settings applied: speed={speed}, stability={stability}, similarity_boost={similarity_boost}")
        except Exception as e:
            print(f"Failed to set voice settings: {e}")

    def generate_voice(self, text, outputfile):
        try:
            # Get the audio as a stream
            audio_stream = self.client.text_to_speech.convert(
                text=text,
                voice_id=self.voice_id,
                model_id=self.model_id,
                output_format="mp3_44100_128",
            )

            # Read the audio data and write to file
            with open(outputfile, 'wb') as f:
                # Convert the generator to bytes
                if hasattr(audio_stream, '__iter__'):  # Check if it's a generator/iterable
                    for chunk in audio_stream:
                        f.write(chunk)
                else:  # If it's already bytes
                    f.write(audio_stream)

            if not os.path.exists(outputfile):
                print(
                    "An error happened during ElevenLabs audio generation, no output audio generated")
                raise Exception(
                    "An error happened during ElevenLabs audio generation, no output audio generated")

            return outputfile

        except Exception as e:
            print("Error generating audio using ElevenLabs:", e)
            raise Exception(
                "An error happened during ElevenLabs audio generation", e)
