"""
Django settings for stable project.

Generated by 'django-admin startproject' using Django 5.0.6.

For more information on this file, see
https://docs.djangoproject.com/en/5.0/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/5.0/ref/settings/
"""

from pathlib import Path

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            # "format": "{asctime} [{levelname:5}] [{process:d}:{thread:d}] {name} - {message}",
            "format": "{asctime} [{levelname:5}] {name} - {message}",
            "style": "{",
        },
    },
    "handlers": {
        "console": {"level": "DEBUG", "class": "logging.StreamHandler", "formatter": "default"},
    },
    "loggers": {
        # "django.db": {"level": "DEBUG"}
        "barn": {"level": "DEBUG"}
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"],
    }
}

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/5.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "django-insecure-y629n4ks%*6g!g8#r*m_sx3f%r*7#ox_n$g@d6!$sz#!#dixq2"

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []


# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",

    "barn",
    "stable.stall",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "stable.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "stable.wsgi.application"


# Database
# https://docs.djangoproject.com/en/5.0/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": BASE_DIR / "db.sqlite3",
        "CONN_MAX_AGE": 60,
        "CONN_HEALTH_CHECKS": True,
        "TIME_ZONE": "UTC",
        "OPTIONS": {
            "timeout": 600,
            # django 5.1
            # "init_command": "PRAGMA journal_mode = WAL; PRAGMA cache = shared; PRAGMA mode = rwc;"
            # "transaction_mode": "IMMEDIATE",
        }
    }
    # "default": {
    #     "ENGINE": "django.db.backends.postgresql",
    #     "NAME": "barn",
    #     "USER": "rds",
    #     "PASSWORD": "sqlsql",
    #     "HOST": "host.docker.internal",
    #     "PORT": "5432",
    #     "CONN_MAX_AGE": 60,
    #     "CONN_HEALTH_CHECKS": True,
    #     "TIME_ZONE": "UTC"
    # }
}


# Password validation
# https://docs.djangoproject.com/en/5.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    # {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    # {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    # {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    # {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]


# Internationalization
# https://docs.djangoproject.com/en/5.0/topics/i18n/

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/5.0/howto/static-files/

STATIC_URL = "static/"

# Default primary key field type
# https://docs.djangoproject.com/en/5.0/ref/settings/#default-auto-field

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

DATA_UPLOAD_MAX_NUMBER_FIELDS = 100000

# BARN
BARN_TASK_SYNC = False
SCHEDULE_POLL_INTERVAL = 5
BARN_TASL_POLL_INTERVAL = 5
