from django import forms

class CSVFileForm(forms.Form):
    uploaded_files = forms.FileField(
        label="Choose Files",
        required=True,
        widget=forms.ClearableFileInput(attrs={
            'multiple': True
        })
    )
