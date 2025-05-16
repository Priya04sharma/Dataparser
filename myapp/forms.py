from django import forms

class CSVFileForm(forms.Form):
    uploaded_files = forms.FileField(
        widget=forms.ClearableFileInput(attrs={'multiple': True}),
        label="Choose Files",
        required=True
    )

    def clean_uploaded_files(self):
        files = self.files.getlist('uploaded_files')
        if not files:
            raise forms.ValidationError("Please upload at least one file.")
        return files
